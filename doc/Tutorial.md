Tutorial
=

Please make sure to install dependencies:

- JDK 7+
- Scala 2.10+
- SBT 0.12.0+

The tutorial assumes that you are using Linux/UNIX operating system.

Below command responses are indicated by "> ".

## Get Aurinko2
    git clone git://github.com/HouzuoGuo/Aurinko2.git
    git checkout alpha
    cd Aurinko2
    sbt test # run all test cases

You may need to increase JVM heap size for SBT to run all test cases. They all should pass.

If you have trouble setting SBT JVM heap size, this [guide][] may help you.

[guide]: http://allstarnix.blogspot.com.au/2013/03/how-to-set-sbt-jvm-parameters-and.html

## Run server, connect client

<table style="width: 100%;">
  <tr>
    <td style="width: 30%">Run server<br/></td>
    <td><pre>CLI options: port_number database_path</pre><pre>sbt "run 1993 /tmp/tutorial_db"</pre></td>
  </tr>
  <tr>
    <td>Connect client</td>
    <td><pre>telnet localhost 1993</pre></td>
  </tr>
</table>

## Collection management

<table style="width: 100%;">
  <tr>
    <td style="width: 30%;">Create collection</td>
    <td><pre>&lt;create col=&quot;os&quot;/&gt;<br/>&lt;go/&gt;<br/>&gt; &lt;ok/&gt;<br/><br/>&lt;create col=&quot;os2&quot;/&gt;<br/>&lt;go/&gt;<br/>&gt; &lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Rename collection</td>
    <td><pre>&lt;rename col=&quot;os2&quot; to=&quot;to_be_deleted&quot;/&gt;<br/>&lt;go/&gt;<br/>&gt; &lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Show all collections</td>
    <td><pre>&lt;all/&gt;<br/>&lt;go/&gt;<br/>&gt; &lt;r&gt;&lt;col&gt;to_be_deleted&lt;/col&gt;&lt;col&gt;os&lt;/col&gt;&lt;/r&gt;<br/>&gt; &lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Drop collection</td>
    <td><pre>&lt;drop col=&quot;to_be_deleted&quot;/&gt;<br/>&lt;go/&gt;<br/>&lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Repair collection from severe data corruption<br/><br/><i>OR recover space from deleted douments</i></td>
    <td><pre>&lt;repair col=&quot;os&quot;/&gt;<br/>&lt;go/&gt;<br/>&lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Shutdown server</td>
    <td><pre>&lt;shutdown/&gt;<br/>&lt;go/&gt;<br/>&gt; &lt;ok/&gt;</pre></td>
  </tr>
</table>


## Document management

<table style="width: 100%;">
  <tr>
    <td style="width: 30%;">Insert document<br/><br/><i>Server responds with document unique ID</id></td>
    <td><pre>&lt;insert&nbsp;col=&quot;os&quot;&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;linux&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;name&gt;Slackware&lt;/name&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;release&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;initial&gt;1993&lt;/initial&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;latest&gt;2012&lt;/latest&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/release&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;/linux&gt;<br/>&lt;/insert&gt;<br/>&lt;go/&gt;<br/>&lt;r&gt;0&lt;/r&gt;<br/>&gt; &lt;ok/&gt;<br/><br/>&lt;insert&nbsp;col=&quot;os&quot;&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;linux&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;name&gt;OpenSUSE&lt;/name&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;release&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;initial&gt;2006&lt;/initial&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;latest&gt;2013&lt;/latest&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/release&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;/linux&gt;<br/>&lt;/insert&gt;<br/>&lt;go/&gt;<br/>&lt;r&gt;482&lt;/r&gt;<br/>&gt; &lt;ok/&gt;<br/><br/>&lt;insert&nbsp;col=&quot;os&quot;&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;UNIX&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;name&gt;Solaris&lt;/name&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;release&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;initial&gt;1992&lt;/initial&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;latest&gt;2012&lt;/latest&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/release&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;/UNIX&gt;<br/>&lt;/insert&gt;<br/>&lt;go/&gt;<br/>&lt;r&gt;961&lt;/r&gt;<br/>&gt; &lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Get all documents</td>
    <td><pre>&lt;findall col=&quot;os&quot;/&gt;<br/>&lt;go/&gt;<br/>&lt;r&gt;&lt;doc id=&quot;0&quot;&gt;&lt;linux&gt;&lt;name&gt;Slackware&lt;/name&gt;&lt;release&gt;&lt;initial&gt;1993&lt;/initial&gt;&lt;latest&gt;2012&lt;/latest&gt;&lt;/release&gt;&lt;/linux&gt;&lt;/doc&gt;&lt;doc id=&quot;482&quot;&gt;&lt;linux&gt;&lt;name&gt;OpenSUSE&lt;/name&gt;&lt;release&gt;&lt;initial&gt;2006&lt;/initial&gt;&lt;latest&gt;2013&lt;/latest&gt;&lt;/release&gt;&lt;/linux&gt;&lt;/doc&gt;&lt;doc id=&quot;961&quot;&gt;&lt;UNIX&gt;&lt;name&gt;Solaris&lt;/name&gt;&lt;release&gt;&lt;initial&gt;1992&lt;/initial&gt;&lt;latest&gt;2012&lt;/latest&gt;&lt;/release&gt;&lt;/UNIX&gt;&lt;/doc&gt;&lt;/r&gt;<br/>&gt; &lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Update document<br/><br/><i>Server responds with new ID of the document (not always the same as original ID)</i></td>
    <td><pre>&lt;update&nbsp;col=&quot;os&quot;&nbsp;id=&quot;961&quot;&gt;<br/>&lt;UNIX&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;name&gt;Solaris&lt;/name&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;release&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;initial&gt;1992&lt;/initial&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;latest&gt;2012&lt;/latest&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;/release&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;vendor&gt;Oracle&lt;/vendor&gt;<br/>&lt;/UNIX&gt;<br/>&lt;/update&gt;<br/>&lt;go/&gt;<br/>&lt;r&gt;&lt;old&gt;961&lt;/old&gt;&lt;new&gt;961&lt;/new&gt;&lt;/r&gt;<br/>&gt; &lt;ok/&gt;</pre></td>
  <tr/>
  <tr>
    <td>Delete document</td>
    <td><pre>&lt;delete col=&quot;os&quot; id=&quot;1234&quot;/&gt;<br/>&lt;go/&gt;<br/>&lt;err&gt;Document 1234 does not exist&lt;/err&gt;<br/>&lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Create index</td>
    <td><pre>&lt;hash-index&nbsp;col=&quot;os&quot;&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;name&lt;/path&gt;<br/>&lt;/hash-index&gt;<br/>&lt;go/&gt;<br/>&gt;&nbsp;&lt;ok/&gt;<br/><br/>&lt;hash-index&nbsp;col=&quot;os&quot;&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;release&lt;/path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;latest&lt;/path&gt;<br/>&lt;/hash-index&gt;<br/>&lt;go/&gt;<br/>&gt;&nbsp;&lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Show all indexes</td>
    <td><pre>&lt;indexed&nbsp;col=&quot;os&quot;/&gt;<br/>&lt;go/&gt;<br/>&gt; &lt;r&gt;&nbsp;&lt;index&nbsp;type=&quot;hash&quot;&nbsp;hash-bits=&quot;12&quot;&nbsp;bucket-size=&quot;100&quot;&gt;&nbsp;&lt;path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;/path&gt;&lt;path&gt;name&lt;/path&gt;&lt;path&gt;<br/>&lt;/path&gt;&lt;/index&gt;&lt;index&nbsp;type=&quot;hash&quot;&nbsp;hash-bits=&quot;12&quot;&nbsp;bucket-size=&quot;100&quot;&gt;&nbsp;&lt;path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;/path&gt;&lt;path&gt;release&lt;/path&gt;&lt;path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;/path&gt;&lt;path&gt;latest&lt;/path&gt;&lt;path&gt;<br/>&lt;/path&gt;&lt;/index&gt;&lt;/r&gt;<br/>&gt; &lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Drop index</td>
    <td><pre>&lt;drop-index&nbsp;col=&quot;os&quot;&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;release&lt;/path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;latest&lt;/path&gt;<br/>&lt;/drop-index&gt;<br/>&lt;go/&gt;<br/>&gt;&nbsp;&lt;ok/&gt;</pre></td>
  </tr>
</table>

## Query

There is a number of query result options:

<table style="width: 100%;">
  <tr>
    <td style="width: 30%;">Document ID</td>
    <td><pre>&lt;q col={ collection }&gt;{ conditions }&lt;/q&gt;</pre></td>
  </tr>
  <tr>
    <td>Document ID and content</td>
    <td><pre>&lt;select col={ collection }&gt;{ conditions }&lt;/select&gt;</pre></td>
  </tr>
  <tr>
    <td>Count of documents</td>
    <td><pre>&lt;count col={ collection }&gt;{ conditions }&lt;/count&gt;</pre></td>
  </tr>
</table>

Conditions are evaluated in the following order: first to last, outter to inner.

### Conditions

<table style="width: 100%;">
  <tr>
    <td style="width: 30%;">Lookup</td>
    <td><pre>&lt;eq&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;to&gt;{&nbsp;value&nbsp;}&lt;/to&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;in&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;{&nbsp;Node&nbsp;name&nbsp;1&nbsp;}&lt;/path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;{&nbsp;Node&nbsp;name&nbsp;2&nbsp;}&lt;/path&gt;...<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;/in&gt;<br/>&lt;/eq&gt;</pre></td>
  </tr>
  <tr>
    <td>Value exists</td>
    <td><pre>&lt;has&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;{&nbsp;Node&nbsp;name&nbsp;1&nbsp;}&lt;/path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;{&nbsp;Node&nbsp;name&nbsp;2&nbsp;}&lt;/path&gt;...<br/>&lt;/has&gt;</pre></td>
  </tr>
  <tr>
    <td>All documents</td>
    <td><pre>&lt;all/&gt;</pre></td>
  </tr>
  <tr>
    <td>Intersection</td>
    <td><pre>&lt;intersect&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;Condition&nbsp;1&nbsp;}<br/>&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;Condition&nbsp;2&nbsp;}...<br/>&lt;/intersect&gt;</pre></td>
  </tr>
  <tr>
    <td>Difference</td>
    <td><pre>&lt;diff&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;Condition&nbsp;1&nbsp;}<br/>&nbsp;&nbsp;&nbsp;&nbsp;{&nbsp;Condition&nbsp;2&nbsp;}...<br/>&lt;/has&gt;</pre></td>
  </tr>
</table>

All of the conditions may take optional `skip` and `limit` attributes.

### Examples

<table style="width: 100%;">
  <tr>
    <td style="width: 30%;">Which OS does not have a latest release in 2012?</td>
    <td><pre>&lt;select&nbsp;col=&quot;os&quot;&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;diff&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;all/&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;eq&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;to&gt;&lt;latest&gt;2012&lt;/latest&gt;&lt;/to&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;in&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;release&lt;/path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;latest&lt;/path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/in&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/eq&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;/diff&gt;<br/>&lt;/select&gt;<br/>&lt;go/&gt;<br/>&gt;&nbsp;&lt;r&gt;&lt;doc&nbsp;id=&quot;482&quot;&gt;&lt;linux&gt;&lt;name&gt;OpenSUSE&lt;/name&gt;&lt;release&gt;&lt;initial&gt;2006&lt;/initial&gt;&lt;latest&gt;2013&lt;/latest&gt;&lt;/release&gt;&lt;/linux&gt;&lt;/doc&gt;&lt;/r&gt;<br/>&gt;&nbsp;&lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Which OS has a latest release in 2012, and was initially released in 1993?</td>
    <td><pre>&lt;select&nbsp;col=&quot;os&quot;&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;intersect&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;eq&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;to&gt;&lt;latest&gt;2012&lt;/latest&gt;&lt;/to&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;in&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;release&lt;/path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;latest&lt;/path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/in&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/eq&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;eq&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;to&gt;&lt;initial&gt;1993&lt;/initial&gt;&lt;/to&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;in&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;release&lt;/path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;path&gt;initial&lt;/path&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/in&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;/eq&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;/intersect&gt;<br/>&lt;/select&gt;<br/>&lt;go/&gt;<br/>&gt;&nbsp;&lt;r&gt;&lt;doc&nbsp;id=&quot;0&quot;&gt;&lt;linux&gt;&lt;name&gt;Slackware&lt;/name&gt;&lt;release&gt;&lt;initial&gt;1993&lt;/initial&gt;&lt;latest&gt;2012&lt;/latest&gt;&lt;/release&gt;&lt;/linux&gt;&lt;/doc&gt;&lt;/r&gt;<br/>&gt;&nbsp;&lt;ok/&gt;</pre></td>
  </tr>
  <tr>
    <td>Give me 2nd and 3rd documents</td>
    <td><pre>&lt;select&nbsp;col=&quot;os&quot;&gt;<br/>&nbsp;&nbsp;&nbsp;&nbsp;&lt;all&nbsp;skip=&quot;1&quot;&nbsp;limit=&quot;2&quot;/&gt;<br/>&lt;/select&gt;<br/>&lt;go/&gt;<br/>&gt;&nbsp;&lt;r&gt;&lt;doc&nbsp;id=&quot;482&quot;&gt;&lt;linux&gt;&lt;name&gt;OpenSUSE&lt;/name&gt;&lt;release&gt;&lt;initial&gt;2006&lt;/initial&gt;&lt;latest&gt;2013&lt;/latest&gt;&lt;/release&gt;&lt;/linux&gt;&lt;/doc&gt;&lt;doc&nbsp;id=&quot;961&quot;&gt;&lt;UNIX&gt;&lt;name&gt;Solaris&lt;/name&gt;&lt;release&gt;&lt;initial&gt;1992&lt;/initial&gt;&lt;latest&gt;2012&lt;/latest&gt;&lt;/release&gt;&lt;vendor&gt;Oracle&lt;/vendor&gt;&lt;/UNIX&gt;&lt;/doc&gt;&lt;/r&gt;<br/>&gt;&nbsp;&lt;ok/&gt;</pre></td>
  </tr>
</table>

# Any question?

[Email me][], I will be very glad to help you!

[Email me]: mailto:guohouzuo@gmail.com
