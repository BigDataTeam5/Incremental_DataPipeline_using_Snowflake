import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import argparse

def generate_key_pair(output_dir, key_size=2048):
    """Generate RSA key pair for Snowflake authentication."""
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate private key
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=key_size,
        backend=default_backend()
    )
    
    # Get public key
    public_key = private_key.public_key()
    
    # Write private key in PEM PKCS8 format (no encryption)
    private_key_path = os.path.join(output_dir, "rsa_key.p8")
    with open(private_key_path, "wb") as f:
        f.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        )
    
    # Write public key
    public_key_path = os.path.join(output_dir, "rsa_key.pub")
    with open(public_key_path, "wb") as f:
        f.write(
            public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
        )
    
    # Create a SQL script to register the public key with Snowflake
    public_key_text = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode('utf-8').strip()
    
    sql_path = os.path.join(output_dir, "register_key.sql") 
    with open(sql_path, "w") as f:
        f.write(f"""-- Execute this SQL in Snowflake to register your public key
ALTER USER YOUR_USERNAME SET RSA_PUBLIC_KEY='{public_key_text}';

-- Verify key registration
DESC USER YOUR_USERNAME;
""")
    
    print(f"Keys generated successfully:")
    print(f"  Private key: {private_key_path}")
    print(f"  Public key: {public_key_path}")
    print(f"  SQL script: {sql_path}")
    print("\nIMPORTANT: Update the SQL script with your username and execute it in Snowflake.")

    return private_key_path, public_key_path, sql_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate RSA key pair for Snowflake authentication")
    parser.add_argument("--output", default="~/.snowflake/keys", help="Output directory for keys")
    parser.add_argument("--key-size", type=int, default=2048, help="Key size in bits")
    args = parser.parse_args()
    
    output_dir = os.path.expanduser(args.output)
    generate_key_pair(output_dir, args.key_size)
