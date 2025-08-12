import os
from openai import OpenAI
from shared.utils.logging_utils import get_logger


class OpenAICleanup:
    def __init__(self):
        self.logger = get_logger(__name__)
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    def delete(self):
        """Cancels all running batches and deletes all files from OpenAI remote storage."""
        self._cancel_all_batches()
        self._delete_all_files()
        self.logger.info("All running batches cancelled and all files deleted.")

    def _cancel_all_batches(self):
        self.logger.info("Fetching all batches...")
        try:
            batches = self.client.batches.list(limit=100)
            for batch in batches.data:
                if batch.status in ["in_progress", "validating", "finalizing"]:
                    self.logger.debug(f"Cancelling batch {batch.id} with status {batch.status}"
                                      )
                    try:
                        self.client.batches.cancel(batch.id)
                        self.logger.debug(f"Batch {batch.id} cancelled.")
                        
                    except Exception as e:
                        self.logger.error(f"Failed to cancel batch {batch.id}: {e}")
                else:
                    self.logger.debug(f"Skipping batch {batch.id} with status {batch.status}")
                    
        except Exception as e:
            self.logger.error(f"Error fetching batches: {e}")

    def _delete_all_files(self):
        self.logger.info("Fetching all files...")
        try:
            files = self.client.files.list()
            
            for file in files.data:
                self.logger.debug(f"Deleting file {file.id} with filename {file.filename}")
                
                try:
                    self.client.files.delete(file.id)
                    self.logger.debug(f"File {file.id} deleted.")
                    
                except Exception as e:
                    self.logger.error(f"Failed to delete file {file.id}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error fetching files: {e}")