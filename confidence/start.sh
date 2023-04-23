hadoop fs -mkdir /user/ehiga
hadoop fs -mkdir /user/ehiga/confidence
hadoop fs -mkdir /user/ehiga/confidence/output1
hadoop fs -put ./input /user/ehiga/confidence
hadoop fs -put ./patterns.txt /user/ehiga/confidence
hadoop fs -put ./stop_words.txt /user/ehiga/confidence
hadoop jar confidence.jar Confidence /user/ehiga/confidence/input /user/ehiga/confidence/output1 /user/ehiga/confidence/output1 /user/ehiga/confidence/output -skip /user/ehiga/confidence/patterns.txt -skip /user/ehiga/confidence/stop_words.txt
#hadoop jar confidence.jar Confidence -D confidence.case.sensitive=false /user/ehiga/confidence/input /user/ehiga/confidence/output -skip /user/ehiga/confidence/patterns.txt -skip /user/ehiga/confidence/stop_words.txt
#hadoop jar confidence.jar Confidence /user/ehiga/confidence/input /user/ehiga/confidence/output -skip /user/ehiga/confidence/patterns.txt
