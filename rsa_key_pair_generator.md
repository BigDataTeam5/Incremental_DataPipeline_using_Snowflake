# Creating rsa key pair authentication method for login into Snowflake for deployments

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
# Register the public key with snowflake user:
    cat ~/.snowflake/keys/rsa_key.pub
# copy the public key into the following snowflake command:
    ALTER USER SHUSHIL SET RSA_PUBLIC_KEY='YOUR_PUBLIC_KEY_STRING_HERE';
# COPY  the private key into the github secrets using the comand:
    (Get-Content "$HOME\.snowflake\keys\rsa_key.p8") -join "\n"
