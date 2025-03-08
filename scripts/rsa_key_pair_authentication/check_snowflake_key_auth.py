import os
import sys
import argparse
import base64
from pathlib import Path
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

def check_key_auth(key_path):
    """
    Check if a private key file is valid for Snowflake authentication.
    
    Args:
        key_path: Path to the private key file
    """
    # Expand the path
    key_path = os.path.expanduser(key_path)
    
    print(f"Checking private key at: {key_path}")
    
    if not os.path.exists(key_path):
        print(f"❌ ERROR: File does not exist: {key_path}")
        return False
    
    print(f"✓ File exists ({os.path.getsize(key_path)} bytes)")
    
    try:
        # Read the file
        with open(key_path, "rb") as key_file:
            key_data = key_file.read()
        
        print(f"✓ File read successfully")
        
        # First 30 chars for debugging
        print(f"Key starts with: {key_data[:30]}")
        
        # Check if it looks like a PEM file
        if not key_data.startswith(b"-----BEGIN"):
            print("❌ ERROR: File doesn't appear to be in PEM format")
            return False
        
        print(f"✓ File appears to be in PEM format")
        
        # Try to load the private key
        private_key = serialization.load_pem_private_key(
            key_data,
            password=None,
            backend=default_backend()
        )
        
        print(f"✓ Key loaded successfully!")
        
        # Convert to DER format required by Snowflake
        pkb = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        print(f"✓ Key converted to DER format successfully!")
        
        # Create a sample connection code
        print("\nSample connection code:")
        print("-----------------------")
        print("import snowflake.connector")
        print("from cryptography.hazmat.backends import default_backend")
        print("from cryptography.hazmat.primitives import serialization")
        print("")
        print(f"# Read private key from {key_path}")
        print("with open(key_path, 'rb') as key_file:")
        print("    p_key = serialization.load_pem_private_key(")
        print("        key_file.read(),")
        print("        password=None,")
        print("        backend=default_backend()")
        print("    )")
        print("")
        print("# Convert to DER format")
        print("pkb = p_key.private_bytes(")
        print("    encoding=serialization.Encoding.DER,")
        print("    format=serialization.PrivateFormat.PKCS8,")
        print("    encryption_algorithm=serialization.NoEncryption()")
        print(")")
        print("")
        print("# Connect to Snowflake")
        print("conn = snowflake.connector.connect(")
        print("    user='YOUR_USERNAME',")
        print("    account='YOUR_ACCOUNT',")
        print("    private_key=pkb")
        print(")")
        
        print("\n✅ The key appears to be valid for Snowflake key-pair authentication!")
        print("   Add it to your GitHub secrets or use it locally.")
        
        return True
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return False

def format_key_for_github(key_path):
    """
    Format a private key for GitHub secrets.
    
    Args:
        key_path: Path to the private key file
    """
    # Expand the path
    key_path = os.path.expanduser(key_path)
    
    if not os.path.exists(key_path):
        print(f"❌ ERROR: File does not exist: {key_path}")
        return
    
    try:
        # Read the file
        with open(key_path, "rb") as key_file:
            key_data = key_file.read()
        
        # Format for GitHub secrets (keep newlines)
        formatted = key_data.decode('utf-8')
        
        print("\nFormatted key for GitHub secrets:")
        print("--------------------------------")
        print(formatted)
        print("--------------------------------")
        print("\nAdd this entire text including BEGIN/END lines and all newlines to your GitHub secret.")
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check if a private key is valid for Snowflake authentication")
    parser.add_argument("--key-path", default="~/.snowflake/keys/rsa_key.p8", help="Path to the private key file")
    parser.add_argument("--format-for-github", action="store_true", help="Format the key for GitHub secrets")
    
    args = parser.parse_args()
    
    if args.format_for_github:
        format_key_for_github(args.key_path)
    else:
        check_key_auth(args.key_path)
