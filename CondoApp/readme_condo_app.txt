Readme for CONDO APP

1. Model deployment workflow filename: "mongodb_...5.py"

2. Model development workflow filename: "houses_3.py"

3. Note these files are converted from colab notebook 
(*.ipynb) to (*.py). 
There may be some un-commenting necessary for files 
to work correctly in Docker or local environment.

4. Refer to requirements.txt, ensure local system has 
necessary packages/libraries installed and services started. 

5. WARNING: environment variables/file handling on local machine 
(as opposed to google colab) needs to be added to all workflows. 
Similarly, filepaths need to be updated as necessary.

6. DOCKER use facilitated by (Dockerfile) and (docker-compose.yml). 
Model deployment workflow is productionized, so expected to be scaled, 
so best used with Docker. 

7. Model development workflow is NOT expected to be scaled. 
So not expected to be used with Docker. Although there may be a 
use-case of transferring the model development workflow to a 
different system. In this case, it is recommended 
that native file (*.py) be shared.

8. Kafka-suggestions: 
Kafka output is ONLY visible in colab notebook cell output.

9. Google Colab how-to:
> Run Model deployment workflow filename: "mongodb_...5.py".
	> ensure no errors
> Access webapp from link in file "web_app_link.txt"
> Run app

____________________________________
> Start Zookeeper server
> Start Kafka server
FAQ/solutions to challenges
challenges: Kafka server keeps shutting down.
solution:
>> Delete subfolders 
~/kafka/data/kafka
~/kafka/data/zookeeper
____________________________________
challenges: How to run zookeeper server (presumed installed on system).
solution:
##open new cmd/pwsh
>> cd d:\kafka

>> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
____________________________________
challenges: How to run Kafka server (presumed installed on system).
solution:
##open new cmd/pwsh
>> cd d:\kafka

>> .\bin\windows\kafka-server-start.bat .\config\server.properties

