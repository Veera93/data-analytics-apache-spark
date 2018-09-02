# Data Analytics Pipeline using Apache Spark

## Project Overview

The goal of this project is to develop a data pipeline which is capable of collecting data from New York Times and storing in in hadoop clusters. Features i.e words were extracted characterizing the category where in only the top N frequently occurring words of each category were selected to be the features. The output feature matrix was fed into the classification step where the model was trained using naive bayes and multi layered perceptron. Testing was performed using random articles and the confusion matrix was studied.

## Data Pipeline
![Flow chart](/images/1_Data_flow.png)

## Data Collection : 
<b>How to run : </b> run the DataCollectionNYTimes.ipynb inside data collection folder<br><br> by providing the topic name which you are interested and folder name to save the articles (Also provide API details)

For this project, articles were collected from New York Times.
The DataCollection.py script file is used to collect the data from NYTimes and save the articles in separate directory based on the category of the article.<br> The topics include Sports, Business, Politics and Science
<br>2000 articles per category were collected
<br>

## Feature Extraction :
<b>How to run : </b> spark-submit wordcount.py data\ collection/ Business Science Sports Politics <br><br>
The input of this process is the location/path of the folder containing the data collected in the previous step. The wordcount.py performs feature extraction, thereby emitting only the features that are required. This is important becasue the unwanted features will affect the accuracy and will take time while training the model. The output of this would be the feature matrix that forms the input for our classification step. The faeature matrix can be found inside the output folder

## Multi class Classification :
 Now that we have extracted the required features, our next step is to train our model to classify the data. We have used two models to train our data. We train the model and the accuracy is reported
   <ul>
    <li> Naive Bayes <br>
        <b>How to run : </b> spark-submit naive_bayes.py output/part-00000 Train<br>
            Test Accuracy : 91.51</li><br>
    <li> Multi-Layer Perceptron <br>
        <b>How to run : </b> spark-submit multilayer_perceptron.py output/part-00000 Train<br>
            Test Accuracy : 92.80</li>
   </ul>

## Testing:
In this phase, we randomly select few article and ask our model to classify. Based on how well we have trained the model in the previous step our testing accuracy will vary.<br>

Save the input files in the folder and run the spark program articlefeatures.py to get the feature vector of the files and we give the input folder and feature file as input to this program<br>
<b>How to run : </b> spark-submit articlefeatures.py input/[TextFileName].txt features/part-00000 <br> where TextFileName is the article and features/part-0000 is where the output is stored
We run both the algorithms and report the results
    
#### Naive Bayes
<b>How to run : </b> spark-submit naive_bayes.py output/part-00000 Test classified/part-00000 <br>   
Naive Bayes Confusion Matrix:
![Naive Bayes](/images/2_Confusion_matrix_Naive_Bayes.png)

#### Multilayer Perceptron
<b>How to run : </b> spark-submit multilayer_perceptron.py output/part-00000 Test classified/part-00000 <br>
Multilayer Perceptron Confusion Matrix:
![Perceptron](/images/3_Confusion_Matrix_Multi_layered.png)



