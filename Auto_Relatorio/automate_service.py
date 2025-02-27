import os
import re
import pandas as pd
import paramiko

# Configurações hardcoded para a conexão AWS
AWS_HOST = "ec2-3-130-73-190.us-east-2.compute.amazonaws.com"
AWS_USER = "ubuntu"
AWS_KEY_PATH = "mailerweb-envya.pem"

class ReportCleaner:
    """
    This class reads a txt file and converts it to a pandas dataframe.
    """
    def __init__(self, file_name, report_type) -> None:
        self.file = file_name
        self.df = None
        self.report_type = report_type
        if file_name:
            self.read_txt_file(file_name)

    def read_txt_file(self, file_name):
        """
        Read a txt file and convert it to a pandas dataframe.
        args:
            file_name: str - the name of the file
        """
        try:
            if self.report_type == "error":
                self.df = pd.read_csv(file_name, sep='\t', header=None)
            else:
                # answer report
                colnames = ['phone_number', 'answer']
                self.df = pd.read_csv(file_name, sep='\t', names=colnames)
        except FileNotFoundError:
            print('File not found')

    def clean_report(self):
        if self.report_type == "error":
            if self.df.columns.size > 2:
                print("### Cleaning five-columns error report ###")
                self.df = self.df[[0, 2]]  # get only the columns with phone number and status
                self.df = self.df[6:]  # remove the first 6 rows
                self.df.columns = ['phone_number', 'status']
                self.df = self.df[self.df['status'] == 'NO']
            else:  # two columns type
                print("### Cleaning two-columns error report ###")
                self.df = self.df[5:]
                self.df.columns = ['phone_number', 'status']
                self.df = self.df[self.df['status'] == 'Não Enviados']
        return self.df

def upload_file_to_aws(file_path, remote_path):
    """
    Uploads a file to AWS instance using SFTP.
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(AWS_HOST, username=AWS_USER, key_filename=AWS_KEY_PATH)

    sftp = ssh.open_sftp()
    sftp.put(file_path, remote_path)
    sftp.close()
    ssh.close()

def run_remote_command(command):
    """
    Runs a command on the AWS instance.
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(AWS_HOST, username=AWS_USER, key_filename=AWS_KEY_PATH)

    stdin, stdout, stderr = ssh.exec_command(command)
    print(stdout.read().decode())
    print(stderr.read().decode())
    ssh.close()

def process_files_in_folder(folder_path):
    """
    Process all files in the specified folder.
    """
    # Regex para extrair o ID da campanha e o tipo de relatório do nome do arquivo
    file_pattern = re.compile(r"(\d+)_(error|answer)\.txt")

    for file_name in os.listdir(folder_path):
        match = file_pattern.match(file_name)
        if match:
            campaign_id = match.group(1)
            report_type = match.group(2)
            file_path = os.path.join(folder_path, file_name)

            print(f"Processing file: {file_name}")
            print(f"Campaign ID: {campaign_id}, Report Type: {report_type}")

            # Step 1: Clean the file
            rc = ReportCleaner(file_name=file_path, report_type=report_type)
            cleaned_df = rc.clean_report()
            output_file = f"{campaign_id}_{report_type}.csv"
            cleaned_df.to_csv(output_file, index=False)

            # Step 2: Upload the file to AWS
            remote_path = f"/home/mailerweb/production/web_app/mailerweb.panel/{output_file}"
            upload_file_to_aws(output_file, remote_path)

            # Step 3: Run the command on AWS
            command = f"python /home/mailerweb/production/web_app/mailerweb.panel/manage.py import_whatsapp_send_{report_type} {campaign_id}_{report_type}.csv {campaign_id} --settings=core.settings.production"
            run_remote_command(command)

            print(f"Finished processing file: {file_name}\n")
        else:
            print(f"Skipping file (does not match pattern): {file_name}")

def main():
    parser = argparse.ArgumentParser(description='Automate the process of cleaning, uploading, and processing reports on AWS.')
    parser.add_argument('--folder', type=str, required=True, help='Path to the folder containing the files')
    args = parser.parse_args()

    # Process all files in the folder
    process_files_in_folder(args.folder)

if __name__ == '__main__':
    main()