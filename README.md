# Demand Model D
Predicts recurring meals using Kaplan-Meier survival curve

## Build
To build the deployment package:

    ./build.sh
    
TODO: automate this using CI tool (store versioned package in artifact repository)

To deploy to AWS Lambda:

    cd src/main/terraform
    terraform init   # only needed the first time you run Terraform in this directory
    terraform workspace select (default|production|...)
    terraform apply -var 'app_version=1.2.3'
    
TODO: automate this using CD tool

## Testing and operations
You can test the default (dev) version of the lambda using the AWS console.
Create a test event with the contents

    {
        "mode": "weekly",
        "ship_dates": ["2019-09-16", "2019-09-17"]
    }

or

    {
        "mode": "hourly"
        "ship_dates": ["2019-09-16", "2019-09-17"]
    }

(pick some dates in the near future, but at least 3 days away)

In production, you can manually trigger the lambda the same way, 
but omit the "ship_dates" field -- it will automatically run for all dates
in the next three weeks.
