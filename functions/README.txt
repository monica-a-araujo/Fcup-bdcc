There are two ways to have a service to periodically call the update endpoint for longest waiting times on our service:

	- Start a job that calls an http endpoint every minute: ./deploy_cronjob.sh

	- Or create a google function that reacts to events and calls the http endpoint: /deploy_function.sh
	  And then create a job that periodically publishes an event what will trigger the google function: ./deploy_pubsub.sh
	  (pubsub topic needs to be created: gcloud pubsub topics create topic)
