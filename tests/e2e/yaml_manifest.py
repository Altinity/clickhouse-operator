import yaml


def get_chi_name(chi_manifest_filename):
    return yaml.safe_load(open(chi_manifest_filename, "r"))["metadata"]["name"]


def get_chit_data(chi_manifest_filename):
    return yaml.safe_load(open(chi_manifest_filename, "r"))

