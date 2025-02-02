package objsets

import TweetReader._

class Tweet(val user: String, val text: String, val retweets: Int) {
  override def toString: String =
    "User: " + user + "\n" +
    "Text: " + text + " [" + retweets + "]"
}

abstract class TweetSet extends TweetSetInterface {

  /**
   * This method takes a predicate and returns a subset of all the elements
   * in the original set for which the predicate is true.
   */
  def filter(p: Tweet => Boolean): TweetSet = filterAcc(p, new Empty)

  /**
   * This is a helper method for `filter` that propagetes the accumulated tweets.
   */
  def filterAcc(p: Tweet => Boolean, acc: TweetSet): TweetSet

  def isEmpty: Boolean
  /**
   * Returns a new `TweetSet` that is the union of `TweetSet`s `this` and `that`.
   */
  def union(that: TweetSet): TweetSet =
    if (that.isEmpty) this
    else filterAcc(tweet => true, that)

  /**
   * Returns the tweet from this set which has the greatest retweet count.
   */
  def mostRetweeted: Tweet

  /**
   * Returns a list containing all tweets of this set, sorted by retweet count
   * in descending order. In other words, the head of the resulting list should
   * have the highest retweet count.
   */
  def descendingByRetweet: TweetList

  /**
   * Returns a new `TweetSet` which contains all elements of this set, and the
   * the new element `tweet` in case it does not already exist in this set.
   *
   * If `this.contains(tweet)`, the current set is returned.
   */
  def incl(tweet: Tweet): TweetSet

  /**
   * Returns a new `TweetSet` which excludes `tweet`.
   */
  def remove(tweet: Tweet): TweetSet

  /**
   * Tests if `tweet` exists in this `TweetSet`.
   */
  def contains(tweet: Tweet): Boolean

  /**
   * This method takes a function and applies it to every element in the set.
   */
  def foreach(f: Tweet => Unit): Unit
}

class Empty extends TweetSet {
  def isEmpty = true
  def filterAcc(p: Tweet => Boolean, acc: TweetSet): TweetSet = acc
  def mostRetweeted: Tweet = throw new java.util.NoSuchElementException
  def descendingByRetweet: TweetList = Nil

  def contains(tweet: Tweet): Boolean = false

  def incl(tweet: Tweet): TweetSet = new NonEmpty(tweet, new Empty, new Empty)

  def remove(tweet: Tweet): TweetSet = this

  def foreach(f: Tweet => Unit): Unit = ()
}

class NonEmpty(elem: Tweet, left: TweetSet, right: TweetSet) extends TweetSet {
  def isEmpty = false

  def filterAcc(p: Tweet => Boolean, acc: TweetSet): TweetSet = {
    val leftB = left.filterAcc(p, acc)
    val rightB = right.filterAcc(p, leftB)
    if(p(elem))
      rightB.incl(elem)
    else rightB
  }

  def mostRetweeted: Tweet = {
    def maxRetweet(t1: Tweet, t2: Tweet):Tweet = if(t1.retweets > t2.retweets) t1 else t2
    if(left.isEmpty && right.isEmpty) elem
    else if (left.isEmpty) maxRetweet(elem, right.mostRetweeted)
    else maxRetweet(elem, left.mostRetweeted)
  }

  def descendingByRetweet: TweetList = {
    val most = this.mostRetweeted
    new Cons(most, this.remove(most).descendingByRetweet)
  }

  def contains(x: Tweet): Boolean =
    if (x.text < elem.text) left.contains(x)
    else if (elem.text < x.text) right.contains(x)
    else true

  def incl(x: Tweet): TweetSet = {
    if (x.text < elem.text) new NonEmpty(elem, left.incl(x), right)
    else if (elem.text < x.text) new NonEmpty(elem, left, right.incl(x))
    else this
  }

  def remove(tw: Tweet): TweetSet =
    if (tw.text < elem.text) new NonEmpty(elem, left.remove(tw), right)
    else if (elem.text < tw.text) new NonEmpty(elem, left, right.remove(tw))
    else left.union(right)

  def foreach(f: Tweet => Unit): Unit = {
    f(elem)
    left.foreach(f)
    right.foreach(f)
  }
}

trait TweetList {
  def head: Tweet
  def tail: TweetList
  def isEmpty: Boolean
  def foreach(f: Tweet => Unit): Unit =
    if (!isEmpty) {
      f(head)
      tail.foreach(f)
    }
}

object Nil extends TweetList {
  def head = throw new java.util.NoSuchElementException("head of EmptyList")
  def tail = throw new java.util.NoSuchElementException("tail of EmptyList")
  def isEmpty = true
}

class Cons(val head: Tweet, val tail: TweetList) extends TweetList {
  def isEmpty = false
}


object GoogleVsApple {
  val google = List("android", "Android", "galaxy", "Galaxy", "nexus", "Nexus")
  val apple = List("ios", "iOS", "iphone", "iPhone", "ipad", "iPad")

  val allTweets = TweetReader.allTweets

  def containsKeyword(tweet: Tweet, keywords: List[String]) : Boolean = {
    if (keywords.isEmpty) false
    else {
      if(tweet.text.contains(keywords.head)) true
      else containsKeyword(tweet, keywords.tail)
    }
  }

  def findTweets(allTweets: TweetSet, keywords: List[String]): TweetSet = {
    if(keywords.isEmpty) allTweets
    else allTweets.filter(tweet => containsKeyword(tweet, keywords))
  }

  def findTweets(keywords: List[String]) = (tweet: Tweet) => keywords.exists(word => tweet.text.contains(word))

  //lazy val googleTweets: TweetSet = allTweets.filter(tweet => google.exists(s => tweet.text.contains(s)))
  //lazy val appleTweets: TweetSet = allTweets.filter(tweet => apple.exists(s => tweet.text.contains(s)))

  lazy val googleTweets: TweetSet = allTweets.filter(findTweets(google))
  lazy val appleTweets: TweetSet = findTweets(allTweets, apple)

  lazy val trending: TweetList = googleTweets.union(appleTweets).descendingByRetweet
}

object Main extends App {
  // Print the trending tweets
  GoogleVsApple.trending foreach println
}
