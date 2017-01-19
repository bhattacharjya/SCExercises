# SCExercises

Requirements: Wordcount on tons of email files with following conditions

1. Only mention words with count more than 1.
2. Output should be sorted by wordcount.
3. Solution should be scalable - could have multiple reducers.
4. Do not count words in the mail headers.



Assumptions/Caveats: 

1. An email header starts with the line having as first word the keyword 'Message-ID:' and ends with the line having as first word the keyword 'X-FileName:'
2. The emails do not have any attachment of any kind.
3. All words are counted, not just valid Nouns or valid words from the dictionary. Further work could be done eliminating invalid words, but is not part of this solution.



Invocation: 

hadoop nameofjarfile.jar exerciseSC.WCMR <inputPath> <outputPath> <numReducers>



Flow:

1. The transformation (class WCMR) has 2 Map reduce jobs 
  a. The first (WCMapper and WCReducer) discards all header information, and counts instances of each word. Any word having less than 10 counts is eliminated. Since there are many individual files, we combine them in a big input split with 'CombineTextInputFormat' input format to optimise input split size. To scale the program allows us to specify number od reducers that we can allow with this job.
  b. The second (SortMapper and SortReducer) takes the many part files produced by the first MR job (number depends on the number of reducers spifies and then sorts them by their wordcount. This second MR job was developed to take care of requirement (2) and (3). If we were able to do with only 1 reducer, then we could have used that to sort by wordcount in the first MR job.


