hadoop fs -mkdir /user/ehiga
hadoop fs -mkdir /user/ehiga/confidence
hadoop fs -put ./input /user/ehiga/confidence
hadoop jar confidence.jar Confidence -Dwordcount.case.sensitive=false /user/ehiga/confidence/input /user/ehiga/confidence/output -skip /user/ehiga/confidence/patterns.txt
