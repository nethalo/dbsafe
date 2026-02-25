Set Shell bash
Set FontSize 14
Set Width 1200
Set Height 1100
Set Theme "Dracula"
Set TypingSpeed 40ms
Set Padding 20

Output assets/dbsafe-idempotent.gif

Hide
Type "export DBSAFE_PASSWORD=dbsafe_demo"
Enter
Show

Sleep 500ms

Type `./dbsafe plan -H 127.0.0.1 -P 23306 -u dbsafe -d demo --idempotent "ALTER TABLE orders ADD COLUMN email VARCHAR(255)"`
Sleep 500ms
Enter
Sleep 4s

Screenshot assets/dbsafe-idempotent.png
