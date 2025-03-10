In the example code, the line with open(env_path, 'w', encoding='utf-8') as env_file: opens the .env file in write mode ("w"), which overwrites the entire file rather than appending or updating only two variables. That is why the entire .env file gets erased and rewritten each time.

If you need to preserve the existing .env content and just update AES_KEY and AES_IV, you have two main options:

1. Open the .env file and parse its existing keys/values, update only AES_KEY and AES_IV, and rewrite the entire file preserving other variables.  
2. Append the new key and IV at the end of the file (which may result in multiple definitions of the same variable unless you remove or override old ones).

Below is an example of how you might preserve existing environment variables when updating AES_KEY and AES_IV. This approach uses python-dotenv to parse the existing .env file and then rewrite it, maintaining all existing variables while only changing AES_KEY and AES_IV.

--------------------------------------------------------------------------------
Example: Generating/Updating AES key/IV in .env without overwriting other variables
--------------------------------------------------------------------------------

from dotenv import load_dotenv, dotenv_values
import os
import secrets
import base64

def generate_or_update_key_iv(env_path='.env'):
    """
    Generate a random 256-bit (32-byte) AES key and a 128-bit (16-byte) IV,
    then update the .env file to preserve other variables.
    """
    # Load existing environment variables from .env (if file exists)
    if os.path.exists(env_path):
        existing_vars = dotenv_values(env_path)
    else:
        existing_vars = {}

    # Generate new key and IV
    key_bytes = secrets.token_bytes(32)  # 256-bit key
    iv_bytes = secrets.token_bytes(16)   # 128-bit IV
    key_b64 = base64.b64encode(key_bytes).decode('utf-8')
    iv_b64 = base64.b64encode(iv_bytes).decode('utf-8')

    # Update existing variables in memory with the new AES values
    existing_vars['AES_KEY'] = key_b64
    existing_vars['AES_IV'] = iv_b64

    # Write them back out to the .env file
    with open(env_path, 'w', encoding='utf-8') as f:
        for key, value in existing_vars.items():
            f.write(f"{key}={value}\n")

    # Also update the current process environment for this session
    os.environ['AES_KEY'] = key_b64
    os.environ['AES_IV'] = iv_b64
