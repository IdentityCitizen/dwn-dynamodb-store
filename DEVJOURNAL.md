# Developers Journal

- 20240711: Added `--timeout 10000` to the mocha command to stop tests from timing out after 2000ms. This was causing issues with the DynamoDB tests when running against a live database rather than a local instance.
- 20240715: Web5 requires splitting a tiebreak (where the sort key is equal) based on messageCid value. This is problematic in DynamoDB since you can only truly sort on one value which is your sort key. I've created indexes for each of the sort variables. To order secondarily based on messageCid is painful as it likely needs to be done client side.

To get around this, I think I'll need to tweak the GSIs and populate item elements with <attribute>+messageCid and sort on that instead. So it'll order on the first section, and on tiebreaks it'll automatically sort using the messageCid tacked onto the end.

# Tips

When running tests and coming across errors, it's a bit difficult to narrow down the problem when the tests are external to this project.

Edit the below file to comment out whole sections of tests. This can reduce a tonne of noise when trying to run individual tests. E.g. Comment out everything except for testMessageStore(); then run tests again.
[text](node_modules/@tbd54566975/dwn-sdk-js/dist/esm/tests/test-suite.js)

You can go even more granular and drop into those methods to add debug logging to individual tests and comment out tests within those modules that aren't necessary.

