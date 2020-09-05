val input = sc.textFile("readme1.txt")
val words = input.flatMap( asdf => asdf.split(" ") )
words.foreach(println)

val wordCount = words.countByValue()