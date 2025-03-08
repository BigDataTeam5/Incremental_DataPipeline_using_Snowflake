import os
import sys
import argparse
from pathlib import Path
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

def setup_keypair_auth(username, account_name, key_dir="~/.snowflake/keys", key_size=2048):
    """
    Set up key pair authentication for Snowflake.
    
    Args:
        username: Snowflake username
        account_name: Snowflake account name
        key_dir: Directory to store keys
        key_size: Size of the RSA key
    """
    # Expand the path
    key_dir = os.path.expanduser(key_dir)
    
    # Create directory if it doesn't exist
    os.makedirs(key_dir, exist_ok=True)
    
    print(f"Generating {key_size}-bit RSA key pair for Snowflake authentication...")
    
    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
        backend=default_backend()
    )
    
    # Get public key
    public_key = private_key.public_key()
    
    # Save private key
    private_key_path = os.path.join(key_dir, "rsa_key.p8")
    with open(private_key_path, "wb") as f:
        f.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        )
    
    # Save public key
    public_key_path = os.path.join(key_dir, "rsa_key.pub")
    with open(public_key_path, "wb") as f:
        f.write(
            public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
        )
    
    # Format public key for Snowflake SQL
    public_key_text = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode("utf-8")
    
    # Remove header and footer
    clean_public_key = "".join(public_key_text.strip().split("\n")[1:-1])
    
    # Create SQL script
    sql_path = os.path.join(key_dir, "register_key.sql")
    with open(sql_path, "w") as f:
        f.write(f"""-- Execute this SQL in Snowflake to register your public key
ALTER USER {username} SET RSA_PUBLIC_KEY='-----BEGIN PUBLIC KEY-----\\n{clean_public_key}\\n-----END PUBLIC KEY-----';

-- Verify key registration
DESC USER {username};
""")

    # Create a test script
    test_script_path = os.path.join(key_dir, "test_connection.py")
    with open(test_script_path, "w") as f:
        f.write(f"""import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Read private key
with open(r"{private_key_path}", "rb") as key:
    p_key = serialization.load_pem_private_key(
        key.read(),
        password=None,
        backend=default_backend()
    )

# Convert to bytes for Snowflake
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Connect to Snowflake with key
conn = snowflake.connector.connect(
    user="{username}",
    account="{account_name}",
    private_key=pkb
)

# Test the connection
cur = conn.cursor()
cur.execute("SELECT current_user(), current_role(), current_version()")
print(cur.fetchone())
cur.close()
conn.close()
print("Connection successful!")
""")

    print(f"\nKey pair generated successfully!")
    print(f"Private key: {private_key_path}")
    print(f"Public key: {public_key_path}")
    print(f"SQL script: {sql_path}")
    print(f"Test script: {test_script_path}")
    print("\nNEXT STEPS:")
    print(f"1. Execute the SQL script in Snowflake to register your public key")
    print(f"2. Run the test script to verify the key authentication works")
    print(f"3. Update your connections.toml file to use key authentication")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Set up key pair authentication for Snowflake")
    parser.add_argument("--username", required=True, help="Snowflake username")
    parser.add_argument("--account", required=True, help="Snowflake account name")
    parser.add_argument("--key-dir", default="~/.snowflake/keys", help="Directory to store keys")
    parser.add_argument("--key-size", type=int, default=2048, help="RSA key size (bits)")
    
    args = parser.parse_args()
    
    setup_keypair_auth(args.username, args.account, args.key_dir, args.key_size)
