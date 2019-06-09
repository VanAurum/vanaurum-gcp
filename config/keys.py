# Quandl API credentials
QUANDL_KEY = "_gwiLzVVwN_g_X5GmFoV"

#Plot.ly API credentials:
PLOTLY_USER='kevinvecmanis'
PLOTLY_KEY='aqW4JOOoPIdsjVbrPjp2'

# GCP settings
DATA_BACKEND = 'datastore'

# Google Cloud Project ID. This can be found on the 'Overview' page at
# https://console.developers.google.com
PROJECT_ID = 'vanaurum'

# CloudSQL & SQLAlchemy configuration
# Replace the following values the respective values of your Cloud SQL
# instance.
CLOUDSQL_USER = 'vanaurum'
CLOUDSQL_PASSWORD = 'ALoqojjrwlq5Mbbw'
CLOUDSQL_DATABASE = 'vanaurum'
# Set this value to the Cloud SQL connection name, e.g.
#   "project:region:cloudsql-instance".
# You must also update the value in app.yaml.
CLOUDSQL_CONNECTION_NAME = 'vanaurum:us-central1:vanaurum'


# Mongo configuration
# If using mongolab, the connection URI is available from the mongolab control
# panel. If self-hosting on compute engine, replace the values below.
MONGO_URI = 'mongodb://user:password@host:27017/database'

# Google Cloud Storage and upload settings.
# Typically, you'll name your bucket the same as your project. To create a
# bucket:
#
#   $ gsutil mb gs://<your-bucket-name>
#
# You also need to make sure that the default ACL is set to public-read,
# otherwise users will not be able to see their upload images:
#
#   $ gsutil defacl set public-read gs://<your-bucket-name>
#
# You can adjust the max content length and allow extensions settings to allow
# larger or more varied file types if desired.
CLOUD_STORAGE_BUCKET = 'vanaurum-blob-data'
MAX_CONTENT_LENGTH = 8 * 1024 * 1024
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg', 'gif', 'csv'])