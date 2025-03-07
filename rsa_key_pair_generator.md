# Creating rsa key pair authentication method for login into Snowflake 

As the account has been upgraded to enterprise level for enabling the external access integration for facilitating outbound network access inside snowflake, there is a necessity to enable mfa to the account.And while deploying, logins are rejected due to this . So instead, we rely on key pair authentication.

# Steps to implement:

# Create directory for keys if it doesn't exist
    mkdir -p ~/.snowflake/keys

# Generate private key
    openssl genrsa -out "$HOME\.snowflake\keys\rsa_key.p8" 2048
Note: if you dont have openssl installed in your system, try running the below commands before:
    choco install openssl


# Generate public key
     openssl rsa -in "$HOME\.snowflake\keys\rsa_key.p8" -pubout -out "$HOME\.snowflake\keys\rsa_key.pub"

# Set proper permissions for private key
     icacls "$HOME\.snowflake\keys\rsa_key.p8" /inheritance:r /grant "$($env:USERNAME):F"