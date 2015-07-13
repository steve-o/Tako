HTTP gateway that fans out a single GET request into multiple TREP-RT snapshots
and serializes the response in JSON.  Connects to NOCACHE_VTA provider Hitsuji
which is embedded within TREP-VA.

Example:

```bash
curl "http://nylabdev5:8000/NKE.N?interval=2013-11-06T14:00:00Z/PT10H&period=PT15M"
```

```javascript
[{"recordname":"/NKE.N?open=1383746400&close=1383747299","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383747300&close=1383748199","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383748200&close=1383749099","fields":{"HIGH_1":77.4500,"LOW_1":76.9600,"OPEN_PRC":77.2300,"HST_CLOSE":77.0000,"ACVOL_1":57928,"NUM_MOVES":92}},
{"recordname":"/NKE.N?open=1383749100&close=1383749999","fields":{"HIGH_1":77.0900,"LOW_1":76.7800,"OPEN_PRC":77.0100,"HST_CLOSE":76.8400,"ACVOL_1":24215,"NUM_MOVES":167}},
{"recordname":"/NKE.N?open=1383750000&close=1383750899","fields":{"HIGH_1":77.1100,"LOW_1":76.8400,"OPEN_PRC":76.8400,"HST_CLOSE":76.8900,"ACVOL_1":22787,"NUM_MOVES":121}},
{"recordname":"/NKE.N?open=1383750900&close=1383751799","fields":{"HIGH_1":76.8700,"LOW_1":76.6200,"OPEN_PRC":76.8700,"HST_CLOSE":76.6300,"ACVOL_1":11688,"NUM_MOVES":91}},
{"recordname":"/NKE.N?open=1383751800&close=1383752699","fields":{"HIGH_1":76.6700,"LOW_1":76.3700,"OPEN_PRC":76.6400,"HST_CLOSE":76.3800,"ACVOL_1":18042,"NUM_MOVES":107}},
{"recordname":"/NKE.N?open=1383752700&close=1383753599","fields":{"HIGH_1":76.4500,"LOW_1":76.1600,"OPEN_PRC":76.3500,"HST_CLOSE":76.3300,"ACVOL_1":15485,"NUM_MOVES":103}},
{"recordname":"/NKE.N?open=1383753600&close=1383754499","fields":{"HIGH_1":76.5000,"LOW_1":76.1900,"OPEN_PRC":76.3100,"HST_CLOSE":76.4900,"ACVOL_1":13297,"NUM_MOVES":91}},
{"recordname":"/NKE.N?open=1383754500&close=1383755399","fields":{"HIGH_1":76.7300,"LOW_1":76.4900,"OPEN_PRC":76.4900,"HST_CLOSE":76.6600,"ACVOL_1":8457,"NUM_MOVES":67}},
{"recordname":"/NKE.N?open=1383755400&close=1383756299","fields":{"HIGH_1":76.7400,"LOW_1":76.5800,"OPEN_PRC":76.6700,"HST_CLOSE":76.5900,"ACVOL_1":10100,"NUM_MOVES":65}},
{"recordname":"/NKE.N?open=1383756300&close=1383757199","fields":{"HIGH_1":76.7400,"LOW_1":76.5900,"OPEN_PRC":76.5900,"HST_CLOSE":76.6500,"ACVOL_1":6837,"NUM_MOVES":58}},
{"recordname":"/NKE.N?open=1383757200&close=1383758099","fields":{"HIGH_1":76.7100,"LOW_1":76.5900,"OPEN_PRC":76.6500,"HST_CLOSE":76.6700,"ACVOL_1":11000,"NUM_MOVES":81}},
{"recordname":"/NKE.N?open=1383758100&close=1383758999","fields":{"HIGH_1":76.7500,"LOW_1":76.6100,"OPEN_PRC":76.6800,"HST_CLOSE":76.6800,"ACVOL_1":16514,"NUM_MOVES":83}},
{"recordname":"/NKE.N?open=1383759000&close=1383759899","fields":{"HIGH_1":76.8500,"LOW_1":76.6900,"OPEN_PRC":76.6900,"HST_CLOSE":76.8000,"ACVOL_1":10573,"NUM_MOVES":69}},
{"recordname":"/NKE.N?open=1383759900&close=1383760799","fields":{"HIGH_1":76.8500,"LOW_1":76.7000,"OPEN_PRC":76.7900,"HST_CLOSE":76.7100,"ACVOL_1":10898,"NUM_MOVES":76}},
{"recordname":"/NKE.N?open=1383760800&close=1383761699","fields":{"HIGH_1":76.7700,"LOW_1":76.6400,"OPEN_PRC":76.7000,"HST_CLOSE":76.6900,"ACVOL_1":11009,"NUM_MOVES":63}},
{"recordname":"/NKE.N?open=1383761700&close=1383762599","fields":{"HIGH_1":76.8200,"LOW_1":76.6200,"OPEN_PRC":76.6800,"HST_CLOSE":76.7500,"ACVOL_1":10465,"NUM_MOVES":82}},
{"recordname":"/NKE.N?open=1383762600&close=1383763499","fields":{"HIGH_1":76.8400,"LOW_1":76.6600,"OPEN_PRC":76.7500,"HST_CLOSE":76.7000,"ACVOL_1":22536,"NUM_MOVES":124}},
{"recordname":"/NKE.N?open=1383763500&close=1383764399","fields":{"HIGH_1":76.7900,"LOW_1":76.6400,"OPEN_PRC":76.7300,"HST_CLOSE":76.7300,"ACVOL_1":29141,"NUM_MOVES":138}},
{"recordname":"/NKE.N?open=1383764400&close=1383765299","fields":{"HIGH_1":76.7700,"LOW_1":76.5700,"OPEN_PRC":76.7400,"HST_CLOSE":76.7200,"ACVOL_1":13292,"NUM_MOVES":87}},
{"recordname":"/NKE.N?open=1383765300&close=1383766199","fields":{"HIGH_1":76.7800,"LOW_1":76.6300,"OPEN_PRC":76.7200,"HST_CLOSE":76.6300,"ACVOL_1":24634,"NUM_MOVES":132}},
{"recordname":"/NKE.N?open=1383766200&close=1383767099","fields":{"HIGH_1":76.6800,"LOW_1":76.5600,"OPEN_PRC":76.6300,"HST_CLOSE":76.5900,"ACVOL_1":9070,"NUM_MOVES":63}},
{"recordname":"/NKE.N?open=1383767100&close=1383767999","fields":{"HIGH_1":76.6600,"LOW_1":76.5600,"OPEN_PRC":76.6100,"HST_CLOSE":76.6200,"ACVOL_1":10929,"NUM_MOVES":77}},
{"recordname":"/NKE.N?open=1383768000&close=1383768899","fields":{"HIGH_1":76.6600,"LOW_1":76.6000,"OPEN_PRC":76.6000,"HST_CLOSE":76.6400,"ACVOL_1":11790,"NUM_MOVES":79}},
{"recordname":"/NKE.N?open=1383768900&close=1383769799","fields":{"HIGH_1":76.7300,"LOW_1":76.6300,"OPEN_PRC":76.6300,"HST_CLOSE":76.6900,"ACVOL_1":10111,"NUM_MOVES":51}},
{"recordname":"/NKE.N?open=1383769800&close=1383770699","fields":{"HIGH_1":76.7100,"LOW_1":76.6000,"OPEN_PRC":76.7100,"HST_CLOSE":76.6800,"ACVOL_1":16123,"NUM_MOVES":90}},
{"recordname":"/NKE.N?open=1383770700&close=1383771599","fields":{"HIGH_1":76.7900,"LOW_1":76.6000,"OPEN_PRC":76.6700,"HST_CLOSE":76.7600,"ACVOL_1":53006,"NUM_MOVES":203}},
{"recordname":"/NKE.N?open=1383771600&close=1383772499","fields":{"HIGH_1":76.7600,"LOW_1":76.7600,"OPEN_PRC":76.7600,"HST_CLOSE":76.7600,"ACVOL_1":97115,"NUM_MOVES":1}},
{"recordname":"/NKE.N?open=1383772500&close=1383773399","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383773400&close=1383774299","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383774300&close=1383775199","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383775200&close=1383776099","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383776100&close=1383776999","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383777000&close=1383777899","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383777900&close=1383778799","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383778800&close=1383779699","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383779700&close=1383780599","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383780600&close=1383781499","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}},
{"recordname":"/NKE.N?open=1383781500&close=1383782399","fields":{"HIGH_1":null,"LOW_1":null,"OPEN_PRC":null,"HST_CLOSE":null,"ACVOL_1":0,"NUM_MOVES":0}}]
```
