[![CodeFactor](https://www.codefactor.io/repository/github/vwt-digital/fttx-dashboard/badge)](https://www.codefactor.io/repository/github/vwt-digital/fttx-dashboard)
# ITH Dashboard

This is the repository of FTTX Dashboard. It contains a Dash dashboard with accompanying files.

## Running the application locally

1. Python environment setup
    ```
    export VENV=~/env
    python3 -m venv $VENV
    source $VENV/bin/activate
    pip install -r requirements.txt
    ```

2. Setting google application credentials  
Download the required service account key from the google cloud console. This key is needed to access resources on the Google Cloud Platform. More info about authentication with a keyfile can be found [here](https://cloud.google.com/docs/authentication/getting-started). Use the following command to set the application credentials locally.

    ```
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/keyfile.json
    ```

3. Disable https for oauth2  
Since we are running on localhost and use oauth2, you will have to disable https enforcement.
    
    ```
    export OAUTHLIB_INSECURE_TRANSPORT=1
    ```

4. Running the application
    ```
    python3 index.py
    ```

## Run with a local firestore

1. Install firebase
https://firebase.google.com/docs/cli/#install-cli-mac-linux

2. Install emulator
https://firebase.google.com/docs/firestore/security/test-rules-emulator#install_the_emulator

3. Install Java (if not installed already)
    Check by running ‘java --version’ on the commandline
    If that results in an error install Java

4. Start Emulator
    ```shell script
    firebase emulators:start --only firestore
    ```

5. Run `local_analysis.py`. This will create a pickle file with data from the firestore on the GCP.

6. Set the environment variables
    ```shell script
    export GOOGLE_CLOUD_PROJECT=vwt-d-gew1-fttx-dashboard
    export FIRESTORE_EMULATOR_HOST=localhost:8080
    ```

7. Run `local_analysis.py` again. Because the environment variables are set the script will now load the local firestore.

8. Do what you wanted to do. Both the analysis and dashboard will connect with the emulated firestore as long as the 
environment variables are set.