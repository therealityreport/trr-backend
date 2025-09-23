# Setting up Google Cloud Credentials in GitHub Codespaces

Your Google Cloud service account credentials need to be set up securely in Codespaces.

## Steps:

1. In your GitHub repository, go to Settings → Secrets and variables → Codespaces
2. Add a new repository secret named `GOOGLE_SERVICE_ACCOUNT_JSON` 
3. Copy the entire contents of your `keys/trr-backend-df2c438612e1.json` file as the value

## The script will automatically create the credentials file in Codespaces from this secret.

Your credentials file contains:
- Service account email
- Private key 
- Project details

Keep this file local and never commit it to git!
