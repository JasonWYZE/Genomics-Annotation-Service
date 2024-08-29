# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

Certainly! Here's your README content structured with Markdown formatting, including a main title and bullet points for easier readability:


## Overview

This project leverages HTML and Flask, a Python web framework, to build a dynamic website that allows users to upload annotation files for processing. The process is as follows:

- When a user uploads a file:
  - The frontend uploads this file to an S3 input bucket.
  - Updates the DynamoDB database to mark the status as "pending".
  - Sends a notification via SQS.
- The backend:
  - Continuously monitors the SQS queue for messages.
  - Upon receiving a message, it downloads the input file from S3 and stores it locally.
  - Runs a subprocess for file processing and updates the job status in DynamoDB to "running".
  - Deletes the message from the queue.
- After processing:
  - A notification triggers an AWS Lambda function which sends an email notification to the user.
  - Updates the job status to "completed".
  - Uploads the processed file to an S3 results bucket.
  - Cleans up any local files.

## Archive Process

- **Free User Processing**: In `announcing/run.py`, once the backend generates the result, it checks if the user is a free user. If so, it sends a message via SQS with a default delay of 5 minutes, allowing the free user to download the result file within this timeframe.
- **Continuous Monitoring**: The script `util/archive/archive.py` runs continuously, monitoring the `yanze41_glacier_archive` SQS queue. Upon receiving a message, it checks the user type:
  - For a **free user**, it downloads the result file from S3, archives it to Glacier, updates DynamoDB with an archive ID, deletes the result file from S3, and finally deletes the message from the queue.
  - For **premium users**, it simply deletes the message without further action. This additional check ensures that any change in user status from premium to free is accounted for.

## Restore Process

- **Initiation**:
  - The restoration process starts when the `/subscribe` endpoint in `web/views.py` sends a POST request to update the user's profile. This triggers an SQS message to the `yanze41_restore` queue.
- **Message Processing**:
  - `util/restore/restore.py` waits for messages from this queue. Upon receipt, it:
    - Skips processing for free users.
    - For premium users, queries DynamoDB for all annotation job records submitted by the user that have been archived to Glacier, specifically those without an `s3_key_result_file` but with a `results_file_archive_id`.
    - Initiates an archive retrieval job for each `results_file_archive_id` on Glacier, attaching the `job_thaw` SNS Topic for notification upon completion. It defaults to expedited retrieval, switching to standard retrieval if faced with `InsufficientCapacityException`.
    - Deletes the message from the `yanze41_restore` queue after processing.
- **Finalization**:
  - `thaw.py` listens on the `yanze41_thaw` queue, subscribed to the `yanze41_thaw` SNS topic. When a job completion message is received, it:
    - Uses the Glacier `JobId` from the message to retrieve the unarchived file.
    - Locates the corresponding DynamoDB record by `results_file_archive_id`, constructs the `s3_key_result_file` name, and uploads the file to S3.
    - Updates the DynamoDB record to delete the `results_file_archive_id` and add the `s3_key_results_file`.
    - Deletes the message from the `yanze41_thaw` queue.

---
