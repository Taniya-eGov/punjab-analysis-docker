"""
Punjab Data Extraction Module
Extracts property tax data from PostgreSQL database to CSV files
"""

import os
import pandas as pd
import psycopg2
from tqdm import tqdm
import logging
from config_loader import ConfigLoader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def format_size(size_bytes):
    """Format file size in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def get_file_size(file_path):
    """Get file size and return formatted string"""
    try:
        size = os.path.getsize(file_path)
        return size, format_size(size)
    except Exception as e:
        logger.warning(f"Could not get size for {file_path}: {e}")
        return 0, "unknown"

def get_directory_size(directory):
    """Get total size of all files in directory"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            try:
                total_size += os.path.getsize(filepath)
            except Exception as e:
                logger.warning(f"Could not get size for {filepath}: {e}")
    return total_size, format_size(total_size)

class PunjabDataExtractor:
    def __init__(self):
        """Initialize extractor using ConfigLoader for secure credential management"""
        # Load configuration from YAML file
        config_path = os.getenv('CONFIG_PATH', '/run/secrets/db_config.yaml')
        environment = os.getenv('ENVIRONMENT', 'production')

        self.config_loader = ConfigLoader(config_path=config_path, environment=environment)
        self.config_loader.validate_config()

        # Get database configuration (no hardcoded credentials!)
        self.db_config = self.config_loader.get_database_config()

        # Get application settings
        self.tenant_id = os.getenv('TENANT_ID', 'pb.adampur')
        data_dirs = self.config_loader.get_data_dirs()
        self.output_dir = os.getenv('OUTPUT_DIR', data_dirs['data_dir'])

        # Get tenant-specific or global settings
        self.chunk_size = self.config_loader.get_setting('chunk_size', default=50000, tenant_id=self.tenant_id)
        self.createdtime_limit = int(os.getenv('CREATEDTIME_LIMIT', '1757269799000'))

        logger.info(f"🔧 Initialized extractor for tenant: {self.tenant_id}")
        logger.info(f"🌍 Environment: {environment}")
        logger.info(f"📁 Output directory: {self.output_dir}")
        logger.info(f"📊 Chunk size: {self.chunk_size}")
        logger.info(f"🔐 Database: {self.db_config['user']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")

    def get_connection(self):
        """Create and return database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            logger.info("Database connection established")
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def create_output_directory(self):
        """Create tenant-specific output directory"""
        tenant_name = self.tenant_id.split('.')[-1]  # Extract 'adampur' from 'pb.adampur'
        self.tenant_dir = os.path.join(self.output_dir, tenant_name)
        os.makedirs(self.tenant_dir, exist_ok=True)
        logger.info(f"Created output directory: {self.tenant_dir}")

    def extract_small_tables(self, conn):
        """Extract smaller tables that don't need chunking"""
        small_tables = [
            'eg_pt_property',
            'eg_pt_owner',
            'eg_pt_unit'
        ]

        for table in small_tables:
            logger.info(f"Extracting table: {table}")

            query = f"""
            SELECT * FROM {table}
            WHERE tenantid = %s
            """

            try:
                df = pd.read_sql_query(query, conn, params=[self.tenant_id])
                output_file = os.path.join(self.tenant_dir, f"{table}.csv")
                df.to_csv(output_file, index=False)

                # Log file size
                file_size_bytes, file_size_str = get_file_size(output_file)
                logger.info(f"✅ Saved {len(df)} records to {table}.csv (Size: {file_size_str})")

            except Exception as e:
                logger.error(f"Failed to extract {table}: {e}")
                raise

    def extract_large_tables(self, conn):
        """Extract large tables with chunking"""
        large_tables = [
            'egbs_demand_v1',
            'egbs_demanddetail_v1'
        ]

        for table in large_tables:
            logger.info(f"Extracting large table: {table}")

            # Create subdirectory for chunks
            table_dir = os.path.join(self.tenant_dir, table)
            os.makedirs(table_dir, exist_ok=True)

            # Get total count for progress tracking
            count_query = f"""
            SELECT COUNT(*) FROM {table}
            WHERE tenantid = %s
            """

            cursor = conn.cursor()
            cursor.execute(count_query, [self.tenant_id])
            total_records = cursor.fetchone()[0]
            logger.info(f"Total records for {self.tenant_id} in {table}: {total_records:,}")

            # Extract in chunks
            chunk_number = 0
            offset = 0

            with tqdm(total=total_records, desc=f"Extracting {table}") as pbar:
                while offset < total_records:
                    chunk_query = f"""
                    SELECT * FROM {table}
                    WHERE tenantid = %s
                    ORDER BY id
                    LIMIT %s OFFSET %s
                    """

                    df_chunk = pd.read_sql_query(
                        chunk_query,
                        conn,
                        params=[self.tenant_id, self.chunk_size, offset]
                    )

                    if df_chunk.empty:
                        break

                    output_file = os.path.join(table_dir, f"output_{chunk_number}.csv")
                    df_chunk.to_csv(output_file, index=False)

                    # Log chunk with size
                    chunk_size_bytes, chunk_size_str = get_file_size(output_file)
                    logger.info(f"Chunk {chunk_number}: {len(df_chunk)} records → output_{chunk_number}.csv ({chunk_size_str})")

                    offset += self.chunk_size
                    chunk_number += 1
                    pbar.update(len(df_chunk))

            # Log total size for this table
            table_size_bytes, table_size_str = get_directory_size(table_dir)
            logger.info(f"✅ {table} complete: {chunk_number} chunks, Total size: {table_size_str}")

            cursor.close()

    def run_extraction(self):
        """Main extraction process"""
        logger.info(f"Starting data extraction for tenant: {self.tenant_id}")

        try:
            # Create output directory
            self.create_output_directory()

            # Get database connection
            conn = self.get_connection()

            # Extract small tables
            self.extract_small_tables(conn)

            # Extract large tables with chunking
            self.extract_large_tables(conn)

            # Close connection
            conn.close()

            # Log total extraction size
            total_size_bytes, total_size_str = get_directory_size(self.tenant_dir)
            logger.info("")
            logger.info("=" * 70)
            logger.info(f"📦 EXTRACTION COMPLETE - Total data size: {total_size_str}")
            logger.info(f"📁 Location: {self.tenant_dir}")
            logger.info("=" * 70)

        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise

if __name__ == "__main__":
    extractor = PunjabDataExtractor()
    extractor.run_extraction()