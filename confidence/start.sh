hadoop fs -mkdir /user/ehiga
hadoop fs -mkdir /user/ehiga/confidence
hadoop fs -put ./input /user/ehiga/confidence
hadoop jar confidence.jar Confidence /user/ehiga/confidence/input /user/ehiga/confidence/output
