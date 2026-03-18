from pathlib import Path
import boto3

# =========================
# REPLACE THESE WITH YOUR REAL VALUES
# =========================
R2_ACCOUNT_ID="a0a9c6a7e45d4ae96756a2db856066fd"
R2_ACCESS_KEY_ID="fd9d27ba0b057574f0749a7cdad5ace0"
R2_SECRET_ACCESS_KEY="5db417d6068fdd77fd6ecac454acf08248687e7132ce43bfc29d7f37ca3783e3"
R2_BUCKET_NAME="telegram-media-bot"
R2_PUBLIC_BASE_URL="https://a0a9c6a7e45d4ae96756a2db856066fd.r2.cloudflarestorage.com"
# Example: https://pub-xxxxxxxxxxxxxxxx.r2.dev
# =========================

R2_ENDPOINT_URL = "https://a0a9c6a7e45d4ae96756a2db856066fd.r2.cloudflarestorage.com"

test_file = Path("test.txt")
test_file.write_text("hello from R2")

object_key = "test.txt"

s3 = boto3.client(
    service_name="s3",
    endpoint_url=R2_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto",
)

with open(test_file, "rb") as f:
    s3.upload_fileobj(
        f,
        R2_BUCKET_NAME,
        object_key,
        ExtraArgs={"ContentType": "text/plain"},
    )

public_url = f"{R2_PUBLIC_BASE_URL}/{object_key}"

print("Upload successful")
print("Public URL:", public_url)