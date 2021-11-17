## Google Analytics Serverless Hit Streaming to Google Cloud

This is not an officially supported Google product.

This solution sends hits directly to a Cloud Function trigger URL, which then uses Pub/Sub and Dataflow to write hits to a table in BigQuery. This table is not date partitioned. Hit data should be available in BigQuery in near real-time.


## Requirements



*   [Google Cloud Platform (GCP) project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) with [billing enabled](https://cloud.google.com/billing/docs/how-to/modify-project#enable-billing) - Create or use an existing project as needed.
    *   **Note**: The examples in this solution use billable GCP resources.
*   [Google Analytics implementation](https://analytics.google.com/analytics/web/) - Ensure that you are deploying Google Analytics directly on the page through analytics.js or through Google Tag Manager.


## Implementation



1. Navigate to your Google Cloud project and open Cloud Shell
2. Enter the following into Cloud Shell



```
rm -rf ga-serverless-streaming && git clone https://github.com/google/ga-serverless-streaming.git && cd ga-serverless-streaming && bash deploy.sh
```



  This will create the following: a cloud function, a cloud storage bucket, a pub/sub topic, and BigQuery dataset and table, and a dataflow job.



3. If you are using analytics.js or Google Tag Manager, take one of the following steps:
    1. **Google Tag Manager**
        1. Enable the built-in referrer variable if it is not already enabled
        2. Create a new custom JavaScript variable in your Google Tag Manager container.
        3. Copy the code from /task\_override/customJSVariable.js and paste it into the new custom JavaScript variable
        4. Replace “CLOUD\_FUNCTION\_TRIGGER\_URL” with the actual cloud function trigger URL.
        5. Add a new field to the Google Analytics Settings variable in the Tag Manager Container. The field name should be “customTask”. The field value should be the custom JavaScript variable that was created.
        6. Save and publish the container.
    2. **Analytics.js**
        7. Copy the code from /task\_override/customGAFunction.js and paste it into the analytics.js snippet between the _[create](https://developers.google.com/analytics/devguides/collection/analyticsjs/command-queue-reference#create)_ and _[send](https://developers.google.com/analytics/devguides/collection/analyticsjs/command-queue-reference#send)_ methods.
        8. Replace “CLOUD\_FUNCTION\_TRIGGER\_URL” with the actual cloud function trigger URL.
        9. Deploy the code.


After following the above steps, you should begin to see Google Analytics data in your newly created BigQuery table.
