1. Install Node.js

2. Clone the repository
```
git clone https://github.com/Kubera2017/tx2.git
```

3. Install dependencies:
```
cd tx2
npm i
```

4. Change Neo4j connection settings in:
```
/src/config.ts
```

5. Build
```
cd tx2
npm run build
```

5. Run script
```
cd tx2
npm run resolve-accounts
```

6. After the script has started you are able to observe duplicates communities:
```
MATCH (n:BankAccountCommunity)-[]-(m) return n, m LIMIT 100
```