from kaggle.api.kaggle_api_extended import KaggleApi
from pathlib import Path

def kaggle_auth():
    """
    This function assumes Kaggle API key set up according to operating system as defined in the documentation.
    :return: Authenticated api connection
    """
    api = KaggleApi()
    api.authenticate()

    return api


def get_file_information(conn, owner, dataset_name):
    """
    Gets information about the dataset files. At present, the returned dictionary is structured as follows:
        {<File Name> : [
            <File Type>,
            <File Size>
            ]
        }
    :param conn: Authenticated KaggleAPI connection
    :param owner: The username of the dataset owner on the Kaggle platform
    :param dataset_name: The name given to the dataset by the owner on the Kaggle platform
    :return: Dictionary structured as described above
    """

    response_dict = conn.datasets_list_files(owner, dataset_name)

    file_info_dict = {}

    for data_file in response_dict['datasetFiles']:
        file_info_dict['nameNullable'] = [data_file['fileTypeNullable'], data_file['totalBytes']]

    return file_info_dict


def download_all_files(conn, dataset_name, save_path, unzip=True):
    """
    Downloads all files associated with a Kaggle dataset. This includes data files, metadata, readme, etc.
    :param conn: Authenticated KaggleAPI connection.
    :param dataset_name: String name given to the dataset by the owner on the Kaggle platform.
    :param save_path: String path to the directory where the files are to be saved.
    :param unzip: Boolean dictating whether or not to unzip the files. Default is True.
    :return: Status Code 200
    """
    conn.dataset_download_files(dataset_name, Path(save_path), unzip=unzip)

    return 200


def download_single_file(conn, dataset_name, save_path, file_name, unzip=True):
    """
    Downloads single file associated with a Kaggle dataset.
    :param conn: Authenticated KaggleAPI connection.
    :param dataset_name: String name given to the dataset by the owner on the Kaggle platform.
    :param save_path: String path to the directory where the files are to be saved.
    :param file_name: String name of the file to be downloaded.
    :param unzip: Boolean dictating whether or not to unzip the files. Default is True.
    :return: Status Code 200
    """

    conn.dataset_download_file(dataset_name, file_name, Path(save_path), unzip=unzip)