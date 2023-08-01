# Topic matching

In an application manifest topics can be described in three ways:

1. As a plain string: All messages from that topic will be set on worterbuchExample:
   `"mytopic"`
2. With a name and an action: The action can be either `set` or `publish`. All messages on that topic will be either set or published to worterbuch, depending on the value. Example: `{
    "name": "mytopic",
    "action": "publish"
}`
3. Using a filter: A filter specifies the name of the topic and one to three possible actions, namely `set`, `publish` and `delete`. The value of each of these is a JSONPath filter expression that will be applied to each message on the topic. If the message matches the filter, then the according action will be triggered based on the following rules:

   - `delete` is only ever be triggered if it is explicitly specified and a message matches the expression
   - if neither `set` nor `publish` are explicitly specified and a message does not match `delete`, then it will be `set`
   - if one of `set` and `publish` is specified explicitly, the other will implicitly catch all messages that do not match an explicitly specified expression, i.e. if only `set` is specified, then all messages not matching its expression will be `publish`ed. If `publish` and `delete` are specified, all messages mathing neither of those will be `set`
   - if `set`, `publish` and `delete` are all theree explicitly specified, then messages that do not match either of them will be ignored
   - if a message matches more than one expression, precedence will be taken in the following order:
     1. set (highest)
     2. publish
     3. delete (lowest)

   Example: `{ "name": "mytopic", "delete": "@.active==false" }` will delete all messages that contain an `active` field with value `false` and `set` all other messages.
