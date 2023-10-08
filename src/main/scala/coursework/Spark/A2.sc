
val tokens = List("I", "am", "very", "happy")
val list = List("apple", "banana", "apple", "cherry", "grapes", "cherry", "grapes", "pears")
list.take(50).distinct

//tokens.flatMap(word1 =>
//  tokens.map(word2 => (word1, word2)).filter { case (word1, word2) => !word1.equals(word2)}
//)

tokens.flatMap(word1 =>
  tokens.flatMap(word2 => if (word1 == word2) Nil else List((word1, Map((word2, 1)))))
)
val lists = List(('a', (1.1, 1)),('b', (2.29, 2)), ('c', (3.312, 3)))
lists.toMap
