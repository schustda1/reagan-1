from googleapiclient.discovery import build
from google.oauth2 import service_account
from reagan.subclass import Subclass
from io import BytesIO
from retrying import retry
from googleapiclient.http import MediaIoBaseDownload

class Drive(Subclass):
    def __init__(self, verbose=0):
        super().__init__(verbose=verbose)
        self.service_account_filepath = self.get_parameter("drive").get_parameter('service_account_path')
        self._create_service()

    @retry(stop_max_attempt_number=10, wait_fixed=10000)
    def _create_service(self):

        api_name = "drive"
        api_version = "v3"

        self.credentials = service_account.Credentials.from_service_account_file(
            self.service_account_filepath
        )
        self.service = build(api_name, api_version, credentials=self.credentials)

    # https://levelup.gitconnected.com/google-drive-api-with-python-part-ii-connect-to-google-drive-and-search-for-file-7138422e0563
    def retrieve_all_files(self):
        results = []
        page_token = None

        while True:
            try:
                param = {}

                if page_token:
                    param['pageToken'] = page_token

                files = self.service.files().list(**param).execute()
                # append the files from the current result page to our list
                results.extend(files.get('files'))
                # Google Drive API shows our files in multiple pages when the number of files exceed 100
                page_token = files.get('nextPageToken')

                if not page_token:
                    break

            except errors.HttpError as error:
                print(f'An error has occurred: {error}')
                break
        # output the file metadata to console
        for file in results:
            self.vprint(file)

        return results

    def download_file(self, file_id, path):
        
        request = self.service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download %d%%." % int(status.progress() * 100))
        with open(path, 'wb') as f:
            f.write(fh.getvalue())


        

if __name__ == "__main__":

    dr = Drive(verbose=1)
    # files = dr.retrieve_all_files()
    # for f in files:
    #     if f['name'] == 'creatives_oba_icon.csv' and f['mimeType'] == 'text/csv':
    #         file_obj = f
    #         break
    
    path = "C:\\Users\\dschuster\\Desktop\\hi.gif"
    file_id = '16w136pklr3LjDf67PSXSo_GzE4Ey_bYg'
    dr.download_file(file_id, path)
    
