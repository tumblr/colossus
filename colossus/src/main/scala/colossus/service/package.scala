package colossus

package object service {
  implicit class Completable[O](val value: O) extends AnyVal {
    def withTags(newtags: (String, String)*): Completion[O] = Completion(value, newtags.toMap)
    def onWrite(w: OnWriteAction): Completion[O] = Completion(value, onwrite = w)
    def complete = Completion(value)
  }
}
