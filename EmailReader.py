import framework
import email
from email.header import decode_header
import os
import zipfile
import imaplib
import pandas as pd
import polars as pl
import numpy as np

class EmailProcessor:
    def __init__(self, email_address, password, imap_server, imap_port, subject_to_search, outputpath,outputfileName):
        self.email_address = email_address
        self.password = password
        self.imap_server = imap_server
        self.imap_port = imap_port
        self.subject_to_search = subject_to_search
        self.outputpath = outputpath
        self.outputfileName = outputfileName

    def connect_to_email_account(self):
        # Connect to the IMAP server
        print('Connect to the IMAP server')
        print(self.imap_server,self.imap_port,type(self.imap_port))
        self.imap_port= int(self.imap_port)
        self.mail = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
        print('Connected IMAP Server')
        if self.mail:
            # Login to the email account
            print('Try Email Login')
            login_result, login_message = self.mail.login(self.email_address, self.password)

            if login_result == 'OK':
                print("Successfully logged in to the email account.")
                return True
            else:
                print(f"Error logging in to the email account: {login_result} - {login_message}")
                return False
        else:
            print(f"Error connecting to the IMAP server")
            return False

    def search_and_process_emails(self):
        if not self.connect_to_email_account():
            return

        # Select the mailbox (in this case, the inbox)
        mailbox = 'INBOX'
        self.mail.select(mailbox)

        # Define search criteria to find emails with a specific subject
        search_criteria = f'(SUBJECT "{self.subject_to_search}")'
        # Search for emails that match the criteria
        search_result, email_ids = self.mail.search(None, search_criteria)
        # self.mail.search(None, f'(SUBJECT "{"ALP PROD P1 : ALP PROD P1 : STFC_Statement of Claim and Transaction Details 09/02/2024 10022024_0601AM"}")')
        # self.mail.uid('SEARCH', None, search_criteria)
        # self.mail.search(None, 'SUBJECT %s' % self.subject_to_search)

        if search_result == 'OK':
            # Check if there is at least one email
            email_id_list = email_ids[0].split()

            email_id_list = sorted(email_id_list, key=lambda x: int.from_bytes(x, byteorder='big'), reverse=True)

            if len(email_id_list) > 0:
                # Create a folder to save the zip attachments
                desktop_path = os.path.expanduser(framework.settings.mftpath)
                folder_name = self.outputpath
                folder_path = os.path.join(desktop_path, folder_name)

                # Create the folder if it doesn't exist
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)

                # Initialize an empty list to store DataFrames
                dataframes = []

                zip_counter = 1  # Reset the counter for each email
                zip_files = []  # Store the ZIP file paths
                # Iterate through email IDs and process each email

                read_all_mails = True
                for email_id in email_id_list:
                    if read_all_mails:
                        fetch_result, email_data = self.mail.fetch(email_id, '(RFC822)')
                    if fetch_result == 'OK':
                        # 'email_data' contains the email content
                        email_message = email_data[0][1]
                        # Parse the email message
                        msg = email.message_from_bytes(email_message)

                        # Decode the subject if it's encoded
                        subject, encoding = decode_header(msg['Subject'])[0]
                        if isinstance(subject, bytes):
                            subject = subject.decode(encoding or 'utf-8')

                        print('self.subject_to_search ',self.subject_to_search)
                        print('subject ', subject)
                        if self.subject_to_search in subject:
                         print(f"Subject: {subject}")

                         #updated_code
                         if 'Custom Settlement Recon Report' not in self.subject_to_search:
                             read_all_mails = False

                         for part in msg.walk():
                            content_disposition = str(part.get("Content-Disposition"))

                            if "attachment" in content_disposition:
                                filename = part.get_filename()
                                if filename:
                                    if filename.lower().endswith(".xlsb"):
                                        attachment_data = part.get_payload(decode=True)
                                        if attachment_data:
                                            # Generate a unique filename for each attachment
                                            unique_filename = f"email_{email_id}_attachment_{zip_counter}.xlsb"
                                            attachment_path = os.path.join(folder_path, unique_filename)

                                            with open(attachment_path, 'wb') as attachment_file:
                                                attachment_file.write(attachment_data)
                                            print(f"Saved xlsb attachment to folder: {attachment_path}")
                                            # Read the CSV file into a DataFrame
                                            df = pd.read_excel(os.path.join(folder_path, unique_filename),engine='pyxlsb',dtype='str')
                                            datecolumns=['InstrDt','Transaction Date','Settlement Date']
                                            for datecol in datecolumns:
                                                if datecol in df.columns:
                                                    df[datecol] = pd.to_datetime('1899-12-30') + pd.to_timedelta(df[datecol].astype(np.float64), unit='D')

                                            dataframes.append(df)  # Append the DataFrame to the list
                                            print(f"Read XLSB file {unique_filename} Data Size {len(df)}.")

                                    if filename.lower().endswith(".xlsx"):
                                        attachment_data = part.get_payload(decode=True)
                                        if attachment_data:
                                            # Generate a unique filename for each attachment
                                            unique_filename = f"email_{email_id}_attachment_{zip_counter}.xlsx"
                                            attachment_path = os.path.join(folder_path, unique_filename)

                                            with open(attachment_path, 'wb') as attachment_file:
                                                attachment_file.write(attachment_data)
                                            print(f"Saved xlsx attachment to folder: {attachment_path}")
                                            # Read the CSV file into a DataFrame
                                            if 'SFL' in self.subject_to_search:
                                                df = pd.read_excel(os.path.join(folder_path, unique_filename),skiprows=1,dtype='str')
                                            else:
                                                df = pd.read_excel(os.path.join(folder_path, unique_filename),dtype='str')
                                            dataframes.append(df)  # Append the DataFrame to the list
                                            print(f"Read XLSX file {unique_filename} Data Size {len(df)}.")

                                    if filename.lower().endswith(".csv"):
                                        attachment_data = part.get_payload(decode=True)
                                        if attachment_data:
                                            # Generate a unique filename for each attachment
                                            unique_filename = f"email_{email_id}_attachment_{zip_counter}.csv"
                                            attachment_path = os.path.join(folder_path, unique_filename)

                                            with open(attachment_path, 'wb') as attachment_file:
                                                attachment_file.write(attachment_data)
                                            print(f"Saved csv attachment to folder: {attachment_path}")
                                            # Read the CSV file into a DataFrame
                                            df = pd.read_csv(attachment_path,dtype='str')
                                            dataframes.append(df)  # Append the DataFrame to the list
                                            print(f"Read CSV file '{unique_filename}' Data Size {len(df)}.")

                                    # Check if the attachment is a zip file
                                    if filename.lower().endswith(".zip"):
                                        attachment_data = part.get_payload(decode=True)

                                        if attachment_data:
                                            # Generate a unique filename for each attachment
                                            unique_filename = f"email_{email_id}_attachment_{zip_counter}.zip"
                                            attachment_path = os.path.join(folder_path, unique_filename)

                                            with open(attachment_path, 'wb') as attachment_file:
                                                attachment_file.write(attachment_data)
                                            print(f"Saved ZIP attachment to folder: {attachment_path}")

                                            zip_files.append(attachment_path)
                                            zip_counter += 1  # Increment the counter for attachments in this email

                # Now, loop through the extracted ZIP files and process them one by one
                for zip_file in zip_files:
                    print(zip_file)
                    try:
                        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                            zipinfos = zip_ref.infolist()
                            for zipinfo in zipinfos:
                                filename = zipinfo.filename
                                if filename.lower().endswith(".csv"):
                                    # Read the CSV file into a DataFrame
                                    with zip_ref.open(filename) as csv_file:
                                        df = pd.read_csv(csv_file,dtype='str')
                                    dataframes.append(df)  # Append the DataFrame to the list
                                    print(f"Read CSV file '{filename}' from {zip_file}.")

                            # Extracted contents of the ZIP file and renamed files, no need to rename them again
                            print(f"Extracted contents of {zip_file} to folder.")

                    except Exception as e:
                        print(f"Error extracting {zip_file}: {str(e)}")
                        return False,  f"Error extracting {zip_file}: {str(e)}"

                # Check if any CSV data was found
                if dataframes:
                    # Concatenate all DataFrames into one
                    combined_df = pd.concat(dataframes, ignore_index=True)
                    # Store the combined DataFrame in the instance variable
                    self.final_dataframe = combined_df

                    # Define the path for the combined CSV file

                    combined_csv_path = os.path.join(folder_path, self.outputfileName)

                    # Save the concatenated DataFrame to a CSV file
                    combined_df.to_csv(combined_csv_path, index=False)

                    print(f"Combined all CSV data into '{combined_csv_path}' Data Size {len(combined_df)}")
                    return True,  f"Combined all CSV data into '{combined_csv_path}' Data Size {len(combined_df)}"
                else:
                    print("No CSV data found in the extracted ZIP files.")
                    return False, "No CSV data found in the extracted ZIP files."
            else:
                print(f"No emails found with the subject '{self.subject_to_search}' in the inbox.")
                return False, f"No emails found with the subject '{self.subject_to_search}' in the inbox."
        else:
            print(f"Error searching for emails: {search_result}")
            return False, f"Error searching for emails: {search_result}"

    def get_combined_dataframe(self):
        return self.final_dataframe


#if __name__ == "__main__":
#     date = '09022024'
#     date1 = pd.to_datetime(date, format='%d%m%Y').strftime('%d %b %y')
#     date2 = pd.to_datetime(date, format='%d%m%Y').strftime('%d/%m/%Y')
#     date3 = pd.to_datetime(date, format='%d%m%Y').strftime('%d.%m.%Y')
#     imap_server = "imap.secureserver.net"
#     imap_user = "alprecon@hellobpcl.in"
#     imap_password = "`130x6/8S{A'_b;"
#     imap_port = '993'

#     #RAZORPAY Payments Report: 09 Feb 24
#     target_substring = f"Custom Payments Report: {date1}"
#     outputpath = f"Custom Payments Report/{date}"
#     # outputfileName = f"Payments Report_{date}.csv"

#    #RAZORPAY Settlement Recon Report: 09 Feb 24
#     target_substring = f"Custom Settlement Recon Report: {date1}"
#     outputpath = f"Custom Settlement Recon Report/{date}"
#     outputfileName = f"Settlement Report_{date}.csv"

#     # ALP PROD P1 : ALP PROD P1 : STFC_Statement of Claim and Transaction Details 09/02/2024 10022024_0601AM
#     target_substring = f"ALP PROD P1 : ALP PROD P1 : STFC_Statement of Claim and Transaction Details {date2}"
#     outputpath = f"STFC_Statement Report/{date}"
#     outputfileName = f"STFC_Statement Report_{date}.csv"

#     # #BPC - ALP for the consumption happened on 09.02.2024, 10.02.2024 & 11.02.2024
#     # target_substring = f"BPC - ALP for the consumption happened on {date3}"
#     # outputpath = f"SFL_Statement Report/{date}"
#     # outputfileName = f"SFL_Statement Report_{date}.csv"

#     # Create an instance of the EmailProcessor class
#     email_processor = EmailProcessor(imap_user, imap_password, imap_server, imap_port, target_substring, outputpath,outputfileName)
#     # Call the method to search and process emails
#     email_processor.search_and_process_emails()
#     df = email_processor.get_combined_dataframe()
#     print(df,len(df))