# Quick Cloud Setup - 15 Minute Guide

## Step 1: Create DigitalOcean Droplet (3 minutes)
1. Go to digitalocean.com → Create Droplet
2. Choose: Ubuntu 22.04, Basic $12/month (2GB RAM)
3. Add your SSH key
4. Click Create

## Step 2: Connect & Install (7 minutes)
```bash
# Connect to your droplet
ssh root@your_droplet_ip

# Install everything (runs automatically)
curl -fsSL https://raw.githubusercontent.com/your-setup/cloud-setup.sh | bash
```

## Step 3: Upload Files (3 minutes)
```bash
# From your local machine
scp v2UniversalSeasonExtractorCastInfo_Parallel_Full.py root@your_ip:~/
scp keys/trr-backend-df2c438612e1.json root@your_ip:~/keys/
```

## Step 4: Start Script (2 minutes)
```bash
# On the server
screen -S cast_scraper
python3 v2UniversalSeasonExtractorCastInfo_Parallel_Full.py
# Press Ctrl+A, then D to detach
exit
```

✅ **Done! Script runs 24/7 without your computer**

## Alternative: Pre-built Image
- Some providers offer pre-configured Python+Chrome images
- This can reduce setup to just 5-10 minutes total

## Cost Breakdown:
- **DigitalOcean**: $12/month ($0.40/day)
- **AWS t3.medium**: ~$30/month ($1/day)  
- **GCP e2-medium**: ~$25/month ($0.83/day)

For a script that might run for days/weeks, even $30/month is much cheaper than keeping your computer on 24/7!
