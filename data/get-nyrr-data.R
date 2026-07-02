# This script is inspired by the Python code originally written by
# bonyejekwe, from the "Marathon_Predictor" repository:
#   https://github.com/bonyejekwe/Marathon_Predictor

library(httr)
library(jsonlite)
library(data.table)

path_runner_ids <- "data/csv/nyrr_runner_ids.csv"
path_runner_results <- "data/csv/nyrr_runner_results.csv"
path_runner_splits <- "data/csv/nyrr_runner_splits.csv"
path_log <- "data/csv/nyrr_scrape.log"

dir.create("data/csv", recursive = TRUE, showWarnings = FALSE)

log_msg <- function(..., only_log = TRUE) {
  line <- sprintf("[%s] %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), sprintf(...))
  if (!only_log) cat(line, "\n")
  cat(line, "\n", file = path_log, append = TRUE)
}

SCRAPE_START <- Sys.time()
MAX_RUNTIME <- 300 * 60

check_runtime <- function() {
  elapsed <- as.numeric(difftime(
    Sys.time(),
    SCRAPE_START,
    units = "secs"
  ))

  if (elapsed >= MAX_RUNTIME) {
    log_msg(
      "Maximum runtime (%.1f min) reached. Stopping scraper.",
      elapsed / 60
    )
    return(TRUE)
  }

  FALSE
}


## get the runners ids for scrapping results -----------------------------------
# return JSON object if request is successful, else return NULL
get_runner_ids <- function(event_code = "M2022", from_place = 1, to_place = 100) {
  url <- "https://rmsprodapi.nyrr.org/api/v2/runners/finishers-filter"

  request_body <- list(
    eventCode = event_code,
    sortColumn = "overallTime",
    sortDescending = FALSE,
    overallPlaceFrom = from_place,
    overallPlaceTo = to_place
  )

  attempts <- 3

  for (attempt in 1:attempts) {
    response <- tryCatch(
      POST(
        url,
        add_headers(`content-type` = "application/json;charset=UTF-8"),
        body = toJSON(request_body, auto_unbox = TRUE),
        encode = "raw",
        timeout(10)
      ),
      error = function(e) NULL
    )

    if (!is.null(response) && status_code(response) == 200) {
      response <- content(response, simplifyVector = FALSE)

      log_msg("received %i items", length(response[["items"]]))
      return(response[["items"]])
    }

    log_msg(
      "Couldn't fetch ids from: %i to: %i, attempt: %i. Trying again...",
      from_place, to_place, attempt,
      only_log = FALSE
    )

    Sys.sleep(1)
  }

  log_msg(
    "Couldn't fetch ids: %i-%i after %i attempts",
    from_place, to_place, attempts,
    only_log = FALSE
  )

  return(NULL)
}

# get the runners results ------------------------------------------------------
# return JSON object if request is successful, else return runnerId
get_runner_results <- function(runner_id) {
  url <- "https://rmsprodapi.nyrr.org/api/v2/runners/resultDetails"

  request_body <- list(runnerId = as.character(runner_id))

  for (attempt in 1:3) {
    response <- tryCatch(
      POST(
        url,
        add_headers(`content-type` = "application/json;charset=UTF-8"),
        body = toJSON(request_body, auto_unbox = TRUE),
        encode = "raw",
        timeout(10)
      ),
      error = function(e) NULL
    )

    if (!is.null(response) && status_code(response) == 200) {
      response <- content(response, simplifyVector = FALSE)

      details <- response[["details"]]

      if (is.null(details)) {
        log_msg("No details found for runner id: %i", runner_id, only_log = FALSE)
        return(NULL)
      }

      splits <- details[["splitResults"]]
      details[["splitResults"]] <- NULL

      log_msg("Received runner details for runner id: %i", runner_id)

      return(list(results = details, splits = splits))
    }

    log_msg(
      "Couldn't fetch runner id: %i, attempt: %i. Trying again...",
      runner_id, attempt,
      only_log = FALSE
    )

    Sys.sleep(1)
  }

  log_msg("Failed to fetch runner id: %i after 3 attempts", runner_id, only_log = FALSE)

  return(NULL)
}

# load all runner ids from NYRR ------------------------------------------------
load_runner_ids <- function(event_code = "M2022", batch_size = 100, wait_sec = 1) {
  from_place <- 1
  total_rows <- 0

  repeat {
    to_place <- from_place + batch_size - 1

    response <- get_runner_ids(event_code, from_place, to_place)

    if (is.null(response)) {
      log_msg(
        "Failed to fetch places %i-%i. Stopping to avoid missing data.",
        from_place, to_place,
        only_log = FALSE
      )
      break
    }

    response <- rbindlist(lapply(response, as.data.table), fill = TRUE)

    fwrite(response, file = path_runner_ids, sep = ",", append = from_place != 1)

    total_rows <- total_rows + nrow(response)
    log_msg("Loaded %i runner ids (total: %i)", nrow(response), total_rows)

    if (nrow(response) == 0) {
      log_msg("Received an empty page. Stopping.")
      break
    } else if (nrow(response) < batch_size) {
      log_msg("Received a partial page. Reached end of results.", only_log = FALSE)
      break
    }

    from_place <- from_place + batch_size
    Sys.sleep(wait_sec)
  }

  log_msg("Done. Total runner ids collected: %i", total_rows, only_log = FALSE)
}


load_runner_results <- function(runner_ids, wait_sec = 0.2) {
  failed_ids <- integer()

  append_results <- file.exists(path_runner_results)
  append_splits <- file.exists(path_runner_splits)

  for (i in seq_along(runner_ids)) {
    if (check_runtime()) {
      log_msg(
        "Stopping after processing %i/%i runners.", i - 1, length(runner_ids),
        only_log = FALSE
      )
      return(invisible(NULL))
    }

    rid <- runner_ids[i]

    response <- get_runner_results(rid)

    if (is.null(response)) {
      failed_ids <- c(failed_ids, rid)

      log_msg("Skipping runnerId = %s", rid, only_log = FALSE)

      Sys.sleep(wait_sec)
      next
    }

    results <- as.data.table(response[["results"]])
    splits <- rbindlist(lapply(response[["splits"]], as.data.table), fill = TRUE)

    splits[, runnerId := rid]
    splits <- dcast(splits, runnerId ~ splitCode, value.var = "time")

    fwrite(results, file = path_runner_results, append = append_results)
    append_results <- TRUE

    fwrite(splits, file = path_runner_splits, append = append_splits)
    append_splits <- TRUE

    log_msg("Processed %i/%i runner ids", i, length(runner_ids))

    Sys.sleep(wait_sec)
  }

  log_msg(
    "Done. Successfully fetched %i/%i runners (%i failed).",
    length(runner_ids) - length(failed_ids), length(runner_ids), length(failed_ids),
    only_log = FALSE
  )

  if (length(failed_ids) > 0) {
    log_msg("Failed runner ids: %s", paste(failed_ids, collapse = ", "))
  }
}

## main ------------------------------------------------------------------------
main <- function() {
  tryCatch(
    {
      log_msg("Starting NYRR scrape run.", only_log = FALSE)

      # Create runner ID file if it doesn't exist
      if (!file.exists(path_runner_ids)) {
        load_runner_ids(wait_sec = 0.5)
      }

      # Always read runner IDs after ensuring the file exists
      runner_ids <- unique(
        fread(path_runner_ids, select = "runnerId")[[1]]
      )

      if (length(runner_ids) == 0) stop("No runner IDs found.")

      # Resume from previous run
      if (file.exists(path_runner_results)) {
        log_msg("Loading processed runner IDs...", only_log = FALSE)

        processed_ids <- unique(
          fread(path_runner_results, select = "runnerId", fill = TRUE)[[1]]
        )

        log_msg("Loaded %i processed runner IDs.", length(processed_ids), only_log = FALSE)

        runner_ids <- setdiff(runner_ids, processed_ids)

        log_msg("%i runners remaining.", length(runner_ids), only_log = FALSE)
      }

      if (length(runner_ids) == 0) {
        log_msg("All runner IDs have already been processed.", only_log = FALSE)
        return(invisible(NULL))
      }

      if (check_runtime()) {
        log_msg("Maximum runtime reached before processing results.", only_log = FALSE)
        return(invisible(NULL))
      }

      load_runner_results(runner_ids, wait_sec = 1)

      log_msg("Scrape run finished successfully.", only_log = FALSE)
    },
    error = function(e) {
      log_msg("Scrape failed: %s", conditionMessage(e), only_log = FALSE)
      stop(e)
    }
  )
}

main()
