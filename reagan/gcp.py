from reagan.subclass import Subclass
from googleapiclient import discovery
from google.oauth2 import service_account


class GCP(Subclass):
    def __init__(self, verbose=0):
        super().__init__(verbose=verbose)
        self.service_account_filepath = self.get_parameter_value("/gcp/service_account_path")
        self.project = self.get_parameter_value("/gcp/project")
        self.region = self.get_parameter_value("/gcp/region")
        self.zone = self.get_parameter_value("/gcp/zone")
        self._create_compute()

    def _create_compute(self):
        SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_filepath, scopes=SCOPES
        )
        self.compute = discovery.build("compute", "v1", credentials=credentials)

    def create_instance(
        self, name, machine_type, source_disk_image, startup_script=None
    ):

        body = {
            "name": name,
            "machineType": f"zones/{self.zone}/machineTypes/{machine_type}",
            # Specify a network interface with NAT to access the public
            # internet.
            "networkInterfaces": [
                {
                    "network": f"https://www.googleapis.com/compute/v1/projects/{self.project}/global/networks/default",
                    "subnetwork": f"https://www.googleapis.com/compute/v1/projects/{self.project}/regions/{self.region}/subnetworks/default",
                    "name": "nic0",
                    "accessConfigs": [
                        {
                            "type": "ONE_TO_ONE_NAT",
                            "name": "external-nat",
                            "networkTier": "PREMIUM",
                        }
                    ],
                }
            ],
            #
            # # Allow the instance to access cloud storage and logging.
            "serviceAccounts": [
                {
                    "email": "default",
                    "scopes": [
                        "https://www.googleapis.com/auth/devstorage.read_write",
                        "https://www.googleapis.com/auth/logging.write",
                    ],
                }
            ],
        }

        if source_disk_image:
            # Specify the boot disk and the image to use as a source.
            body["disks"] = [
                {
                    "boot": True,
                    "autoDelete": True,
                    "initializeParams": {"sourceImage": source_disk_image},
                }
            ]

        if startup_script:
            # Metadata is readable from the instance and allows you to
            # pass configuration from deployment scripts to instances.
            body["metadata"] = {
                "items": [
                    {
                        # Startup script is automatically executed by the
                        # instance upon startup.
                        "key": "startup-script-url",
                        "value": startup_script,
                    }
                ]
            }

        return (
            self.compute.instances()
            .insert(project=self.project, zone=self.zone, body=body)
            .execute()
        )

    def list_to_df(self):
        request = self.compute.instances().list(project=self.project, zone=self.zone)
        response = request.execute()
        return self._json_to_df(response.get('items'))

    def delete_instance(self, name):
        return self.compute.instances().delete(
            project=self.project,
            zone=self.zone,
            instance=name).execute()

if __name__ == "__main__":
    gcp = GCP()
    # name = "test-instance21"
    # machine_type = "g1-small"
    # source_disk_image = "https://www.googleapis.com/compute/v1/projects/us-gm-175021/global/images/adstxt-image"
    # instance = gcp.create_instance(name=name,machine_type=machine_type,source_disk_image=source_disk_image)
    # images = gcp.list_to_df()
    # deleted_image = gcp.delete_instance(name)
