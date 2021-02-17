from googleapiclient.discovery import build
from google.oauth2 import service_account
from reagan.subclass import Subclass
from io import BytesIO
import pandas as pd
from time import sleep


class SA360(Subclass):
    def __init__(self, version="v2", verbose=0):
        super().__init__(verbose=verbose)
        self.version = version
        self.service_account_filepath = self.get_parameter_value("/sa360/service_account_path")
        self._create_service()
        self.dcm_api_calls = 0

    def _create_service(self):

        api_name = "doubleclicksearch"
        self.credentials = service_account.Credentials.from_service_account_file(
            self.service_account_filepath
        )
        self.service = build(api_name, self.version, credentials=self.credentials)

    def get_report_fragments(self, report_id):
        """
        Returns a list containing the fragments (ints) of a report
            - report_id (int): Id of the Report used to make the calls
        """

        request = self.service.reports().get(reportId=report_id)
        report_status = request.execute()
        while True:
            if report_status["isReportReady"]:
                return [
                    report_file["url"].split("/")[-1]
                    for report_file in report_status["files"]
                ]
            request = self.service.reports().get(reportId=report_id)
            report_status = request.execute()
        sleep(60)

    def file_to_df(self, report_id, report_fragment):
        """
        Returns a pandas dataframe given a Report Id and Fragment
            - report_id (int): Id of the Report used to make the calls
            - report_fragment (int): Fragment file of the report
        """

        request = self.service.reports().getFile(
            reportId=report_id, reportFragment=report_fragment
        )
        report_file = request.execute()
        return pd.read_csv(BytesIO(report_file))

    def reports_to_df(self, agency_id, report_type, columns):
        """
        Returns a generator that yields a pandas dataframe with 1000000 rows with the report specifications
            - agency_id (int): Id of the Agency used to make the calls
            - report_type (string): Specified report type
            - columns (list): Columns to include in the report
        """

        body = {
            "reportScope": {"agencyId": agency_id},
            "reportType": report_type,
            "columns": [{"columnName": col} for col in columns],
            "downloadFormat": "csv",
            "maxRowsPerFile": 1000000,
            "statisticsCurrency": "agency",
        }

        # 1. Request Report
        report_request = self.service.reports().request(body=body)
        report = report_request.execute()
        report_id = report["id"]

        # 2. Wait for it to finish
        report_fragments = self.get_report_fragments(report_id)

        # 3. Download files
        for report_fragment in report_fragments:
            yield self.file_to_df(report_id=report_id, report_fragment=report_fragment)

    def decode_error(self, error):
        # Returns a more concise error description
        return eval(error.content.decode())["error"]["message"]


if __name__ == "__main__":
    # pass
    # account report
    # report_type = 'campaign'
    # agency_id = 20700000001049589
    # columns = ['campaignId','campaign','campaignStartDate','campaignEndDate']
    sa = SA360(verbose=1)
    for df in sa.reports_to_df():
        pass
