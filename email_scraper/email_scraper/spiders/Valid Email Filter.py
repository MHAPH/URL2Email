import pandas as pd

def filter_and_modify_emails(input_csv, output_csv):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_csv)

    # Filter out rows where the email doesn't contain a domain part (no '.' after '@')
    valid_email_mask = df['Email'].str.contains(r'@\w+\.\w+', na=False)
    df_valid_emails = df[valid_email_mask]

    # Remove duplicate emails
    df_valid_emails = df_valid_emails.drop_duplicates(subset='Email')

    # Rename the 'Email' column to 'Recipient'
    df_valid_emails.rename(columns={'Email': 'Recipient'}, inplace=True)

    # Add a new column 'Email Sent' and keep it empty
    df_valid_emails['Email Sent'] = ''

    # Save the filtered, deduplicated, and modified DataFrame to a new CSV file
    df_valid_emails.to_csv(output_csv, index=False)

# Specify the input and output file paths
input_csv = r'C:\Users\muham\Downloads\URL-Email\USA_HOSPITAL_2.csv'
output_csv = r'C:\Users\muham\Downloads\URL-Email\filtered_output_file.csv'

# Call the function to filter invalid emails, remove duplicates, rename the column, and add the new column
filter_and_modify_emails(input_csv, output_csv)
