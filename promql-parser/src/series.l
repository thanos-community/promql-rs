%x BRACES SERIES

%%

[ \t\n\r]+ ;
[a-zA-Z_][a-zA-Z_0-9]*:[a-zA-Z_0-9:]* <SERIES>"METRIC_IDENTIFIER"
[a-zA-Z_][a-zA-Z_0-9:]* <SERIES>"IDENT"
\{ <BRACES>"LBRACE"

<BRACES>[ \t\n\r]+ ;
<BRACES>[a-zA-Z_][a-zA-Z_0-9]* "IDENT_LABEL"
<BRACES>("([^"\\\n]|\\.)*"|'([^'\\\n]|\\.)*'|`[^`]*`) "STRING"
<BRACES>= "EQ"
<BRACES>, "COMMA"
<BRACES>\} <SERIES>"RBRACE"

<SERIES>[ \t]+ "SPACE"
<SERIES>\{ <BRACES>"LBRACE_AFTER_NAME"
<SERIES>x "TIMES"
<SERIES>_ "BLANK"
<SERIES>stale "STALE"
<SERIES>([0-9]+\.[0-9]+([eE][+\-]?[0-9]+)?|[0-9]+([eE][+\-]?[0-9]+)?|\.[0-9]+([eE][+\-]?[0-9]+)?|[Nn][Aa][Nn]|[Ii][Nn][Ff]) "NUMBER"
<SERIES>\+ "ADD"
<SERIES>- "SUB"
