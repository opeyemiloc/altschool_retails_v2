"""
Ingestion module for downloading and verifying Kaggle datasets.
"""
from .kaggle_client import KaggleClient
from .downloader import DatasetDownloader, download_retail_dataset

__all__ = ["KaggleClient", "DatasetDownloader", "download_retail_dataset"]
