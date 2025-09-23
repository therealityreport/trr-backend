# Cloud Setup Guide for Cast Info Scraper

## Option 1: AWS EC2 (Recommended)

### 1. Launch EC2 Instance
- Instance type: t3.medium (2 vCPU, 4GB RAM)
- OS: Ubuntu 22.04 LTS
- Storage: 20GB GP3
- Security group: Allow SSH (port 22)

### 2. Connect and Setup
```bash
# Connect to instance
ssh -i your-key.pem ubuntu@your-ec2-ip

# Update system
sudo apt update && sudo apt upgrade -y

# Install Chrome
wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" | sudo tee /etc/apt/sources.list.d/google-chrome.list
sudo apt update
sudo apt install google-chrome-stable -y

# Install Python and pip
sudo apt install python3 python3-pip python3-venv -y

# Install dependencies
pip3 install selenium gspread

# Install virtual display (for headless Chrome)
sudo apt install xvfb -y
```

### 3. Upload Your Script
```bash
# Upload via SCP
scp -i your-key.pem v2UniversalSeasonExtractorCastInfo_Parallel_Full.py ubuntu@your-ec2-ip:~/
scp -i your-key.pem keys/trr-backend-df2c438612e1.json ubuntu@your-ec2-ip:~/keys/
```

### 4. Run with Screen (Persistent Session)
```bash
# Start screen session
screen -S cast_scraper

# Set display for headless Chrome
export DISPLAY=:99
Xvfb :99 -screen 0 1920x1080x24 &

# Run your script
python3 v2UniversalSeasonExtractorCastInfo_Parallel_Full.py

# Detach from screen: Ctrl+A, then D
# Reattach later: screen -r cast_scraper
```

## Option 2: DigitalOcean Droplet

### 1. Create Droplet
- Image: Ubuntu 22.04 LTS
- Size: Basic ($12/month - 2 vCPU, 2GB RAM)
- Add SSH key

### 2. Same setup as EC2 above

## Option 3: Google Cloud Platform

### 1. Create VM Instance
```bash
gcloud compute instances create cast-scraper \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --machine-type=e2-medium \
    --zone=us-central1-a
```

### 2. Follow same setup steps as EC2

## Script Modifications for Cloud

### Make Chrome Headless
Add this to your Chrome options in the script:
```python
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
```

### Add Logging
```python
import logging
logging.basicConfig(
    filename='cast_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
```

### Add Progress Persistence
Save progress to a file so you can resume if interrupted:
```python
import json

def save_progress(processed_rows):
    with open('progress.json', 'w') as f:
        json.dump({'processed_rows': processed_rows}, f)

def load_progress():
    try:
        with open('progress.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {'processed_rows': []}
```

## Cost Estimates
- **AWS EC2 t3.medium**: ~$30/month
- **DigitalOcean Basic Droplet**: $12/month
- **GCP e2-medium**: ~$25/month

## Monitoring
- Check logs: `tail -f cast_scraper.log`
- Check screen session: `screen -list`
- Monitor resources: `htop`
