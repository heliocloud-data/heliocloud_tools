"""
Constants used in HelioCloud lambdas.
"""

# Default AWS Region to use.  Comes in handy with identifying the location of certain AWS services
# (ex: S3 buckets)
DEFAULT_AWS_REGION = "us-east-1"

# Default ARN for a Pandas Lambda Layer (used by multiple HelioCloud lambdas)
DEFAULT_PANDA_LAYERS_ARN = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python39:6"
