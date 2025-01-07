import os
import subprocess
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from discord_webhook import DiscordWebhook

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('backup.log')
    ]
)

@dataclass
class BackupConfig:
    """Store backup configuration settings"""
    use_git: bool
    use_discord: bool
    db_user: str
    db_password: str
    db_name: str
    export_dir: Path
    webhook_url: Optional[str]
    backup_interval_hours: int
    retention_period_hours: int

    @classmethod
    def from_user_input(cls) -> 'BackupConfig':
        """Create config from user input with validation"""
        try:
            use_git = input("Use Git for backups? (yes/no): ").lower().startswith('y')
            use_discord = input("Use Discord webhook? (yes/no): ").lower().startswith('y')
            
            config = {
                'use_git': use_git,
                'use_discord': use_discord,
                'db_user': input("Database username: ").strip(),
                'db_password': input("Database password: ").strip(),
                'db_name': input("Database name: ").strip(),
                'export_dir': Path(input("Backup directory path: ").strip()),
                'webhook_url': input("Discord webhook URL: ").strip() if use_discord else None,
                'backup_interval_hours': int(input("Backup interval (hours): ")),
                'retention_period_hours': int(input("Retention period (hours): "))
            }
            
            # Validate inputs
            if not config['db_user'] or not config['db_password'] or not config['db_name']:
                raise ValueError("Database credentials cannot be empty")
            
            if config['backup_interval_hours'] <= 0 or config['retention_period_hours'] <= 0:
                raise ValueError("Time intervals must be positive")
                
            return cls(**config)
            
        except ValueError as e:
            logging.error(f"Configuration error: {e}")
            raise

class DatabaseBackup:
    """Handles database backup operations"""
    
    def __init__(self, config: BackupConfig):
        self.config = config
        self.git_repo_url = "https://github.com/zaid-ahmed-001/DatabaseBackup-test"
        self._setup_export_directory()

    def _setup_export_directory(self) -> None:
        """Create export directory if it doesn't exist"""
        self.config.export_dir.mkdir(parents=True, exist_ok=True)
        
        if self.config.use_git:
            self._setup_git_repo()

    def _setup_git_repo(self) -> None:
        """Initialize and configure Git repository"""
        try:
            if not (self.config.export_dir / '.git').exists():
                os.chdir(self.config.export_dir)
                subprocess.run(['git', 'init'], check=True, capture_output=True)
                subprocess.run(['git', 'remote', 'add', 'origin', self.git_repo_url], 
                             check=True, capture_output=True)
                subprocess.run(['git', 'pull', 'origin', 'main'], 
                             check=True, capture_output=True)
                logging.info("Git repository initialized successfully")
        except subprocess.CalledProcessError as e:
            logging.error(f"Git setup failed: {e.stderr.decode()}")
            raise

    def create_backup(self) -> Optional[Path]:
        """Create database backup and return file path"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = self.config.export_dir / f"{self.config.db_name}_{timestamp}.sql"
        
        try:
            command = [
                "mysqldump",
                "--ssl=0",
                "-u", self.config.db_user,
                f"--password={self.config.db_password}",
                self.config.db_name
            ]
            
            subprocess.run(command, stdout=backup_file.open('w'),
                         check=True, capture_output=True)
            logging.info(f"Backup created: {backup_file}")
            return backup_file
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Backup failed: {e.stderr.decode()}")
            return None

    def send_to_discord(self, backup_file: Path) -> None:
        """Send backup to Discord"""
        if not self.config.use_discord or not self.config.webhook_url:
            return
            
        try:
            webhook = DiscordWebhook(
                url=self.config.webhook_url,
                username="Database Backup Bot"
            )
            
            webhook.add_file(
                file=backup_file.read_bytes(),
                filename=backup_file.name
            )
            
            response = webhook.execute()
            if response.status_code == 200:
                logging.info("Backup sent to Discord")
            else:
                logging.error(f"Discord webhook failed: {response.status_code}")
                
        except Exception as e:
            logging.error(f"Discord upload failed: {e}")

    def commit_to_git(self, backup_file: Path) -> None:
        """Commit and push backup to Git"""
        if not self.config.use_git:
            return
            
        try:
            os.chdir(self.config.export_dir)
            subprocess.run(['git', 'add', backup_file], check=True, capture_output=True)
            commit_msg = f"Backup created: {datetime.now().isoformat()}"
            subprocess.run(['git', 'commit', '-m', commit_msg], check=True, capture_output=True)
            subprocess.run(['git', 'push', 'origin', 'main'], check=True, capture_output=True)
            logging.info("Backup pushed to Git")
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Git operations failed: {e.stderr.decode()}")

    def cleanup_old_backups(self) -> None:
        """Remove backups older than retention period"""
        cutoff_time = datetime.now() - timedelta(hours=self.config.retention_period_hours)
        
        for backup_file in self.config.export_dir.glob('*.sql'):
            if backup_file.stat().st_mtime < cutoff_time.timestamp():
                try:
                    backup_file.unlink()
                    logging.info(f"Deleted old backup: {backup_file}")
                except Exception as e:
                    logging.error(f"Cleanup failed for {backup_file}: {e}")

def main():
    """Main backup routine"""
    try:
        config = BackupConfig.from_user_input()
        backup_manager = DatabaseBackup(config)
        
        while True:
            logging.info("Starting backup process...")
            
            if backup_file := backup_manager.create_backup():
                if config.use_git:
                    backup_manager.commit_to_git(backup_file)
                if config.use_discord:
                    backup_manager.send_to_discord(backup_file)
                    
                backup_manager.cleanup_old_backups()
                
            wait_minutes = config.backup_interval_hours * 60
            logging.info(f"Waiting {wait_minutes} minutes until next backup...")
            time.sleep(wait_minutes * 60)
            
    except KeyboardInterrupt:
        logging.info("Backup process stopped by user")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

if __name__ == "__main__":
    main()