# gmail-mails-size

A tool to build a local database of everything in your Gmail mailbox

## Setup

### Dependencies
You just need python (>= 3.6), and package xlsxwriter\
(`pip install xlsxwriter`)

### Gmail API
You will need a Google Developer Console project, \
and credentials associated with it.\
Download them into this folder as `credentials.json`.

## Usage
This script entirely works on the command line.\
It takes a main argument, action, which determines the action it must perform. 

Help can be displayed using
```
./GmailTools.py --help
```

### Actions
- `auth`: Simply authenticates user through Gmail API, and print his profile
- `step1`: Download messages IDs, then messages content, and save them in a DB
- `step2`: For each message retrieved from step1, downloads attachments and \
saves message data and attachments content in final database (root directory)
- `step3`: Generate ZIP file containing user's DB and generated excel file
- `run`: Performs steps 1, 2 and 3 one after the other
- `clean`: Remove temp DB files (generated at step1) \
and, optionally, ZIP & attachments files
- `labels-usage-count`: Displays the number of emails per label, \
using local data
- `count-mails-in-label`: Displays the number of mails for this label, \
using latest Gmail data
- `delete-from-label--offline`: Deletes all mails from intersection of \
specified labels, based on local data
- `delete-from-label`: Same as `delete-from-label--offline` except that it \
uses latest Gmail data
- `delete-from-list`: Delete messages from theirs IDs

### Use cases

- Download all mails data, including attachments
```
./GmailTools.py --action run
```

- Remove ***locally*** all mails owned by authenticated user :
```
./GmailTools.py --action clean
```

- Delete ***permanently, on Gmail's side*** mails in trash:
```
./GmailTools.py --action delete-from-label --labels-ids TRASH
```

- Delete ***permanently, on Gmail's side*** mails in ***both*** labels INBOX \
and UNREAD:
```
./GmailTools.py --action delete-from-label --labels-ids INBOX,UNREAD
```

- Delete ***permanently, on Gmail's side*** mails identified by these IDs:
```
./GmailTools.py --action delete-from-list --messages-ids id1,id2,id3
```

### Optional parameters
- `-p`, `--port`: Port on which the script will listen when authenticating \
on Gmail's API
- `-tf`, `--token-file`: File in which user's token is stored. Can be useful \
to switch between many accounts within the same folder.
- `-gac`, `--google-api-credentials`: Gmail Developer credentials (JSON string)
- `-mids`, `--messages-ids`: Used for action `delete-from-list`
- `-lids`, `--labels-ids`: Used for action `delete-from-label[--offline]`
- `-th`, `--threads`: Number of threads in which steps 1.2 and 2 are run
- `-ca`, `--console-auth`: Do Gmail API's auth in console (by pasting a token) \
instead of configuring an HTTP server
- `-y`, `--yes`: Don't ask for any user's confirmation

## Authors

* **Carlotronics** - *Initial work* - [Carlotronics](https://github.com/Carlotronics)