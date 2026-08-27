package downloadqueue

import (
	"aura/database"
	"aura/logging"
	"aura/mediaserver"
	"aura/mediux"
	"aura/models"
	sonarr_radarr "aura/sonarr-radarr"
	"aura/utils"
	"context"
	"fmt"
)

// ProcessNextQueueJob atomically claims the next pending Download Queue Job
// and processes it fully (synchronously). Returns false if there was no
// pending job to claim.
func ProcessNextQueueJob(ctx context.Context) (processed bool) {
	// The worker runs on a bare background context, unlike HTTP handlers, so
	// create a logging context here for the DB calls below to attach to.
	ctx, ld := logging.CreateLoggingContext(ctx, "Download Queue Worker")
	claimAction := ld.AddAction("Claim Next Download Queue Job", logging.LevelDebug)
	ctx = logging.WithCurrentAction(ctx, claimAction)

	job, found, claimErr := database.ClaimNextDownloadQueueJob(ctx, workerID)
	claimAction.Complete()
	if claimErr.Message != "" {
		logging.LOGGER.Error().Timestamp().Str("error", claimErr.Message).Msg("Failed to claim next Download Queue Job")
		ld.Log()
		return false
	}
	if !found {
		ld.Log()
		return false
	}

	QueueBroadcaster.Publish(QueueEvent{Type: "job_started", Job: &job})

	processQueueJob(ctx, job)
	return true
}

func processQueueJob(ctx context.Context, job database.DownloadQueueJob) {
	ctx, ld := logging.CreateLoggingContext(ctx, "Download Queue Processing")
	subAction := ld.AddAction(fmt.Sprintf("Processing Job %d: %s", job.ID, job.MediaItemTitle), logging.LevelInfo)
	ctx = logging.WithCurrentAction(ctx, subAction)
	defer ld.Log()
	defer subAction.Complete()

	queueItem := job.Payload

	fileErrors := []string{}
	fileWarnings := []string{}

	finalize := func() {
		status := "success"
		if len(fileErrors) > 0 {
			status = "error"
		} else if len(fileWarnings) > 0 {
			status = "warning"
		}

		message := fmt.Sprintf("%s (%s)", queueItem.MediaItem.Title, queueItem.MediaItem.LibraryTitle)

		finishErr := database.FinishDownloadQueueJob(ctx, job.ID, status, message, fileErrors, fileWarnings)
		if finishErr.Message != "" {
			subAction.AppendWarning(fmt.Sprintf("job_%d", job.ID), "Failed to finalize Download Queue Job")
		}

		finishedJob := job
		finishedJob.Status = status
		finishedJob.ResultMessage = message
		finishedJob.ResultErrors = fileErrors
		finishedJob.ResultWarnings = fileWarnings
		QueueBroadcaster.Publish(QueueEvent{Type: "job_finished", Job: &finishedJob})

		// Clear the job from the queue regardless of outcome. Download History
		// is now the permanent record, so the queue only shows active jobs.
		_ = database.DeleteDownloadQueueJob(ctx, job.ID)
	}

	if queueItem.MediaItem.RatingKey == "" || queueItem.MediaItem.Title == "" || queueItem.MediaItem.LibraryTitle == "" || queueItem.MediaItem.TMDB_ID == "" {
		reason := "Media Item missing required fields: ratingKey/title/libraryTitle/tmdbId"
		fileErrors = append(fileErrors, reason)
		recordDownloadHistoryFailure(ctx, queueItem.MediaItem, "", "", reason)
		finalize()
		return
	}

	if len(queueItem.PosterSets) == 0 {
		reason := "No poster sets found"
		fileWarnings = append(fileWarnings, reason)
		recordDownloadHistoryFailure(ctx, queueItem.MediaItem, "", "", reason)
		finalize()
		return
	}

	mediuxItemInfo, mErr := mediux.GetBaseItemInfoByTMDB_ID(queueItem.MediaItem.TMDB_ID, queueItem.MediaItem.Type)
	if mErr.Message != "" {
		fileWarnings = append(fileWarnings, fmt.Sprintf("MediUX lookup failed: %s", mErr.Message))
	}

	found, mediaErr := mediaserver.GetMediaItemDetails(ctx, &queueItem.MediaItem)
	if mediaErr.Message != "" || !found {
		fileErrors = append(fileErrors, mediaErr.Message)
		recordDownloadHistoryFailure(ctx, queueItem.MediaItem, "", "", mediaErr.Message)
		finalize()
		return
	}

	for _, posterSet := range queueItem.PosterSets {
		setErrors := []string{}
		setWarnings := []string{}
		imageResults := []models.ImageDownloadResult{}

		if posterSet.ID == "" || posterSet.Type == "" || posterSet.Title == "" {
			setErrors = append(setErrors, "poster set missing required fields: id/type/title")
			fileErrors = append(fileErrors, setErrors...)
			recordDownloadHistoryFailure(ctx, queueItem.MediaItem, posterSet.ID, posterSet.Title, setErrors[0])
			SendNotification(
				FileIssues{Errors: setErrors, Warnings: setWarnings},
				queueItem.MediaItem,
				posterSet,
				mediuxItemInfo.TMDB_PosterPath,
				mediuxItemInfo.TMDB_BackdropPath,
			)
			continue
		}

		if !posterSet.SelectedTypes.Poster &&
			!posterSet.SelectedTypes.Backdrop &&
			!posterSet.SelectedTypes.SeasonPoster &&
			!posterSet.SelectedTypes.SpecialSeasonPoster &&
			!posterSet.SelectedTypes.Titlecard {
			setWarnings = append(setWarnings, "poster set has no selected image types")
			fileWarnings = append(fileWarnings, setWarnings...)
			recordDownloadHistoryFailure(ctx, queueItem.MediaItem, posterSet.ID, posterSet.Title, setWarnings[0])
			SendNotification(
				FileIssues{Errors: setErrors, Warnings: setWarnings},
				queueItem.MediaItem,
				posterSet,
				mediuxItemInfo.TMDB_PosterPath,
				mediuxItemInfo.TMDB_BackdropPath,
			)
			continue
		}

		for idx, image := range posterSet.Images {
			switch image.Type {
			case "poster":
				if !posterSet.SelectedTypes.Poster {
					continue
				}
			case "backdrop":
				if !posterSet.SelectedTypes.Backdrop {
					continue
				}
			case "season_poster":
				if image.SeasonNumber == nil {
					continue
				}
				// Check if the Media Item contains the season number for this image, if not skip it
				mediaItemHasSeason := false
				if queueItem.MediaItem.Series != nil {
					for _, season := range queueItem.MediaItem.Series.Seasons {
						if *image.SeasonNumber == season.SeasonNumber {
							mediaItemHasSeason = true
							break
						}
					}
				}
				if !mediaItemHasSeason {
					continue
				}
				if *image.SeasonNumber == 0 {
					if !posterSet.SelectedTypes.SpecialSeasonPoster {
						continue
					}
				} else {
					if !posterSet.SelectedTypes.SeasonPoster {
						continue
					}
				}
			case "titlecard":
				// Check if the Media Item contains the Season and Episode numbers for this image, if not skip it
				mediaItemHasEpisode := false
				if queueItem.MediaItem.Series != nil {
					for _, season := range queueItem.MediaItem.Series.Seasons {
						for _, episode := range season.Episodes {
							if image.SeasonNumber != nil && *image.SeasonNumber != season.SeasonNumber {
								continue
							}
							if image.EpisodeNumber != nil && *image.EpisodeNumber != episode.MediuxEpisodeNumber {
								continue
							}
							mediaItemHasEpisode = true
							break
						}
						if mediaItemHasEpisode {
							break
						}
					}
				}
				if !mediaItemHasEpisode {
					continue
				}
				if !posterSet.SelectedTypes.Titlecard {
					continue
				}
			default:
				subAction.AppendWarning(fmt.Sprintf("job_%d_image_%d", job.ID, idx), fmt.Sprintf("Image has unrecognized type '%s'", image.Type))
				fileWarnings = append(fileWarnings, fmt.Sprintf("Image '%s' has unrecognized type '%s'", image.Src, image.Type))
				continue
			}

			QueueBroadcaster.Publish(QueueEvent{Type: "job_progress", Progress: &QueueProgress{
				JobID:          job.ID,
				MediaItemTitle: queueItem.MediaItem.Title,
				ImageType:      image.Type,
				SeasonNumber:   image.SeasonNumber,
				EpisodeNumber:  image.EpisodeNumber,
			}})

			downloadFileName := utils.GetFileDownloadName(queueItem.MediaItem.Title, image)
			downloadErr := mediaserver.DownloadApplyImageToMediaItem(ctx, &queueItem.MediaItem, image)
			result := models.ImageDownloadResult{
				ImageType:     image.Type,
				SeasonNumber:  image.SeasonNumber,
				EpisodeNumber: image.EpisodeNumber,
				Success:       downloadErr.Message == "",
			}
			if downloadErr.Message != "" {
				setErrors = append(setErrors, fmt.Sprintf("%s: %s", downloadFileName, downloadErr.Message))
				result.FailureReason = downloadErr.Message
			}
			imageResults = append(imageResults, result)
		}

		// Per-set notification (success/warning/error)
		SendNotification(
			FileIssues{Errors: setErrors, Warnings: setWarnings},
			queueItem.MediaItem,
			posterSet,
			mediuxItemInfo.TMDB_PosterPath,
			mediuxItemInfo.TMDB_BackdropPath,
		)

		recordDownloadHistory(ctx, queueItem.MediaItem, posterSet.ID, posterSet.Title, imageResults)

		fileErrors = append(fileErrors, setErrors...)
		fileWarnings = append(fileWarnings, setWarnings...)
	}

	upsertErr := database.UpsertSavedItem(ctx, queueItem)
	if upsertErr.Message != "" {
		fileErrors = append(fileErrors, upsertErr.Message)
		recordDownloadHistoryFailure(ctx, queueItem.MediaItem, "", "", upsertErr.Message)
		finalize()
		return
	}

	finalize()

	// Handle any labels and tags asynchronously
	go func() {
		ctx, ld := logging.CreateLoggingContext(context.Background(), "Download Queue - Labels and Tags Handling")
		logAction := ld.AddAction("Handle Labels and Tags for Added Item", logging.LevelInfo)
		ctx = logging.WithCurrentAction(ctx, logAction)
		defer ld.Log()
		selectedTypes := models.SelectedTypes{}
		for _, posterSet := range queueItem.PosterSets {
			selectedTypes.Poster = selectedTypes.Poster || posterSet.SelectedTypes.Poster
			selectedTypes.Backdrop = selectedTypes.Backdrop || posterSet.SelectedTypes.Backdrop
			selectedTypes.SeasonPoster = selectedTypes.SeasonPoster || posterSet.SelectedTypes.SeasonPoster
			selectedTypes.SpecialSeasonPoster = selectedTypes.SpecialSeasonPoster || posterSet.SelectedTypes.SpecialSeasonPoster
			selectedTypes.Titlecard = selectedTypes.Titlecard || posterSet.SelectedTypes.Titlecard
		}

		mediaserver.AddLabelToMediaItem(ctx, queueItem.MediaItem, selectedTypes)
		sonarr_radarr.HandleTags(ctx, queueItem.MediaItem, selectedTypes)
	}()
}

func recordDownloadHistory(ctx context.Context, mediaItem models.MediaItem, setID, setTitle string, imageResults []models.ImageDownloadResult) {
	succeeded := 0
	failedImages := []models.ImageDownloadResult{}
	for _, r := range imageResults {
		if r.Success {
			succeeded++
		} else {
			failedImages = append(failedImages, r)
		}
	}

	if succeeded == 0 && len(failedImages) == 0 {
		// Nothing was actually attempted for this set (e.g. all images filtered out); skip history.
		return
	}

	entry := database.DownloadHistoryEntry{
		TMDB_ID:         mediaItem.TMDB_ID,
		LibraryTitle:    mediaItem.LibraryTitle,
		Edition:         mediaItem.Edition,
		MediaItemTitle:  mediaItem.Title,
		MediaItemYear:   mediaItem.Year,
		SetID:           setID,
		SetTitle:        setTitle,
		ImagesSucceeded: succeeded,
		ImagesFailed:    len(failedImages),
		FailedImages:    failedImages,
	}

	if insertErr := database.InsertDownloadHistoryEntry(ctx, entry); insertErr.Message != "" {
		logging.LOGGER.Error().Timestamp().Str("error", insertErr.Message).Msg("Failed to insert Download History entry")
	}
}

func recordDownloadHistoryFailure(ctx context.Context, mediaItem models.MediaItem, setID, setTitle, reason string) {
	recordDownloadHistory(ctx, mediaItem, setID, setTitle, []models.ImageDownloadResult{
		{FailureReason: reason},
	})
}
