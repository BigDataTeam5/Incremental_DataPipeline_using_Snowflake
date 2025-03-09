#!/usr/bin/env python3
"""
Check the connections.toml file and output its structure to diagnose issues.
"""
import os
import sys
import toml

def main():
    config_path = os.path.expanduser("~/.snowflake/connections.toml")
    print(f"Looking for connections.toml at: {config_path}")
    
    if not os.path.exists(config_path):
        print(f"ERROR: File does not exist: {config_path}")
        return False
    
    print(f"File exists, size: {os.path.getsize(config_path)} bytes")
    print(f"File permissions: {oct(os.stat(config_path).st_mode)[-3:]}")
    
    try:
        with open(config_path, 'r') as f:
            content = f.read()
            print("\nFile content preview (first 200 chars):")
            print(content[:200])
            
        # Try loading with toml
        print("\nParsing with toml:")
        config = toml.load(config_path)
        print(f"Profiles found: {list(config.keys())}")
        
        # Check each profile
        for profile, settings in config.items():
            print(f"\nProfile: {profile}")
            for key, value in settings.items():
                # Don't print sensitive values
                if key in ['password', 'private_key']:
                    print(f"  {key}: [REDACTED]")
                else:
                    print(f"  {key}: {value}")
        
        return True
    except Exception as e:
        print(f"ERROR parsing file: {str(e)}")
        
        # Try to read the raw file content
        try:
            with open(config_path, 'rb') as f:
                binary_content = f.read(100)
                print(f"\nBinary content (first 100 bytes): {binary_content}")
        except Exception as e2:
            print(f"ERROR reading raw file: {str(e2)}")
        
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
