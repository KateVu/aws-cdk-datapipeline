import os.path as path
import yaml


def load_config(file_name="config.yaml", config_folder="../config/"):
    """
    Load configuration from the specified YAML file.

    :param file_name: Name of the YAML file to load.
    :param config_folder: Path to the configuration folder.
    :return: Dictionary containing configuration values.
    """
    config_path = path.join(path.dirname(__file__), f"{config_folder}/{file_name}")
    with open(config_path, "r") as config_file:
        return yaml.safe_load(config_file)


def get_config_account(account_name, file_name="aws_account.yaml", config_folder="../config/"):
    """
    Get account configuration based on the account name from the YAML configuration file.

    :param account_name: Name of the AWS account (e.g., sandpit1, sandpit2)
    :param file_name: Name of the YAML file containing account mappings.
    :param config_folder: Path to the configuration folder.
    :return: Account object as a dictionary.
    """
    accounts = load_config(file_name, config_folder)
    for account in accounts:
        if account["name"] == account_name:
            return account
    raise ValueError(f"Account name '{account_name}' not found in {file_name}")