own_peer_ids <- c(
  "12D3KooWBkcbgU2GLxVPnYmg9esWnXYwJzj8FC5TsDJQKJibPNZL", # Monitor DE2
  "QmQVPbfxcYwDCegXDr5kw7KU6kCxCMV6JFeVPyhJe9BNh1", # Monitor DE1
  "QmWwwJdCzk8UiDo8W5bAuSNM4nzwHrTYN2UhC7DW6UsHsQ", # Indexer 1 on Monitor DE1 machine
  "QmQifMCfrv79TNbLdMYXza8cbETKGCrfxSQqRQjXSEMrkJ", # Indexer 2 on Monitor DE1 machine
  "QmSDA9WHwyHsJRbuw9D95GfNTvfXHxRtLjEzywSdPtjDps", # Monitor US1
  "QmSrAwsMHmGYD2688JQNQxxtgHpLuGvT4NqnoBc7qBaQ6i", # Indexer on Monitor US1 machine
  "QmYSNXDuWE9FrVjEYfmeSxgS6eo1t4cvFgZ36TVv3nNn72", # Indexer 0 on institute VM
  "QmeK5Fb93tTBT1LmA1qcMaaRBucftomecDZeUKrWUP2Wp2", # Indexer 1 on institute VM
  "QmUfw5Tfj4LNDUScusrsSmP43xCe4ap4tsQ4Aac6SU6Cnf", # Indexer 2 on institute VM
  "QmVPCend8Z6hCmnkrgwk2haAWmVi8uVRfsPs4fVYFuFhwW", # Indexer 3 on institute VM
  "QmY7T9tKDhmRNFXAxre4QHeZtryUAMtCnCkNmQfcCEQHm5", # Indexer 4 on institute VM
  "QmeVKku4fo12QudwbmNwRFk7MwqG7aFsJra6T2f1vZkBG2", # Indexer 5 on institute VM
  "QmXXJqataoDkfBhT8EpPkrUdj2NUBs11WLth4w2gL2zN6F", # Indexer 6 on institute VM
  "QmcRhmooxTE4ipT67Q87fK8K276cmsDWDQfXos9qNpRaAs", # Indexer 7 on institute VM
  "QmV4ajJdyk4JkbJGm7QvAbt3rHMAEo33ESc9CM6GE9QZXW", # Indexer 8 on institute VM
  "QmYkBFCpLDyeQ3wsfbhokLXRRN21c78xAGKztJAe7icuUp", # Indexer 9 on institute VM
  "QmdZhTV965PYFtu3URYh2XvJF2YZJiuXUZ7AaMyMTfE8Lg"  # Big indexer on institute VM with lots of storage
)

cf_peer_ids <- c(
  "QmNjm16wkUUsUoRmr3b8QoQAPZYBwfBHcygkjGbSauTSWu", # /ip4/172.65.0.13/tcp/4001/p2p/QmNjm16wkUUsUoRmr3b8QoQAPZYBwfBHcygkjGbSauTSWu
  "QmSfPx5uNa1Jh4Rn7LTtV6uaiVoWP7MjvMdzAKEM2R9swJ", # /ip4/172.65.0.13/tcp/4002/p2p/QmSfPx5uNa1Jh4Rn7LTtV6uaiVoWP7MjvMdzAKEM2R9swJ
  "QmSUobMzQkFHYty1ABu7B6MtbYaDCK9APm4irtgg96xrhm", # /ip4/172.65.0.13/tcp/4003/p2p/QmSUobMzQkFHYty1ABu7B6MtbYaDCK9APm4irtgg96xrhm
  "QmdAXatABqmgZGZ8LJMMaCT3esQvpTPPqH3Pk9ZjDDA3JW", # /ip4/172.65.0.13/tcp/4004/p2p/QmdAXatABqmgZGZ8LJMMaCT3esQvpTPPqH3Pk9ZjDDA3JW
  "QmcywxEW3uxg3MQWrfWywVSS7YuhMvYuzBLQxvdGcjGzSS", # /ip4/172.65.0.13/tcp/4005/p2p/QmcywxEW3uxg3MQWrfWywVSS7YuhMvYuzBLQxvdGcjGzSS
  "Qmb53qh7fo26k99u3KATf4nFDUbg2tnUPdfhAqp63jU67S", # /ip4/172.65.0.13/tcp/4006/p2p/Qmb53qh7fo26k99u3KATf4nFDUbg2tnUPdfhAqp63jU67S
  "QmQPF396xeGSnrcZSSqSVJGigerAoCmnnAyzu8E8zGxQ7D", # /ip4/172.65.0.13/tcp/4007/p2p/QmQPF396xeGSnrcZSSqSVJGigerAoCmnnAyzu8E8zGxQ7D
  "QmPcDM7QGkQZGc2EV12CiG6STbFF7G5w4gsFv4v4m48o6k", # /ip4/172.65.0.13/tcp/4008/p2p/QmPcDM7QGkQZGc2EV12CiG6STbFF7G5w4gsFv4v4m48o6k
  "QmYAXDCy9sZCEZFmQtFi6f5a7JbBoWJsixdHzrKuNPxumv",  # /ip4/172.65.0.13/tcp/4009/p2p/QmYAXDCy9sZCEZFmQtFi6f5a7JbBoWJsixdHzrKuNPxumv
  # new peer IDs:
  "QmcF5yUHqZYWDZYEdZRK8XALRtQxNqJ5w2jPecGDK4jUxZ",  # /ip4/172.65.0.13/tcp/4001/p2p/QmcF5yUHqZYWDZYEdZRK8XALRtQxNqJ5w2jPecGDK4jUxZ
  "QmcFf2FH3CEgTNHeMRGhN7HNHU1EXAxoEk6EFuSyXCsvRE",  # /ip4/172.65.0.13/tcp/4002/p2p/QmcFf2FH3CEgTNHeMRGhN7HNHU1EXAxoEk6EFuSyXCsvRE
  "QmcFmLd5ySfk2WZuJ1mfSWLDjdmHZq7rSAua4GoeSQfs1z",  # /ip4/172.65.0.13/tcp/4003/p2p/QmcFmLd5ySfk2WZuJ1mfSWLDjdmHZq7rSAua4GoeSQfs1z
  "QmcfFmzSDVbwexQ9Au2pt5YEXHK5xajwgaU6PpkbLWerMa",  # /ip4/172.65.0.13/tcp/4004/p2p/QmcfFmzSDVbwexQ9Au2pt5YEXHK5xajwgaU6PpkbLWerMa
  "QmcfJeB3Js1FG7T8YaZATEiaHqNKVdQfybYYkbT1knUswx",  # /ip4/172.65.0.13/tcp/4005/p2p/QmcfJeB3Js1FG7T8YaZATEiaHqNKVdQfybYYkbT1knUswx
  "QmcfVvzK4tMdFmpJjEKDUoqRgP4W9FnmJoziYX5GXJJ8eZ",  # /ip4/172.65.0.13/tcp/4006/p2p/QmcfVvzK4tMdFmpJjEKDUoqRgP4W9FnmJoziYX5GXJJ8eZ
  "QmcfZD3VKrUxyP9BbyUnZDpbqDnT7cQ4WjPP8TRLXaoE7G",  # /ip4/172.65.0.13/tcp/4007/p2p/QmcfZD3VKrUxyP9BbyUnZDpbqDnT7cQ4WjPP8TRLXaoE7G
  "QmcfZP2LuW4jxviTeG8fi28qjnZScACb8PEgHAc17ZEri3",  # /ip4/172.65.0.13/tcp/4008/p2p/QmcfZP2LuW4jxviTeG8fi28qjnZScACb8PEgHAc17ZEri3
  "QmcfgsJsMtx6qJb74akCw1M24X1zFwgGo11h1cuhwQjtJP",  # /ip4/172.65.0.13/tcp/4009/p2p/QmcfgsJsMtx6qJb74akCw1M24X1zFwgGo11h1cuhwQjtJP
  "Qmcfr2FC7pFzJbTSDfYaSy1J8Uuy8ccGLeLyqJCKJvTHMi",  # /ip6/2606:4700:60::6/tcp/4010/p2p/Qmcfr2FC7pFzJbTSDfYaSy1J8Uuy8ccGLeLyqJCKJvTHMi
  "QmcfR3V5YAtHBzxVACWCzXTt26SyEkxdwhGJ6875A8BuWx",  # /ip6/2606:4700:60::6/tcp/4011/p2p/QmcfR3V5YAtHBzxVACWCzXTt26SyEkxdwhGJ6875A8BuWx
  "Qmcfuo1TM9uUiJp6dTbm915Rf1aTqm3a3dnmCdDQLHgvL5",  # /ip6/2606:4700:60::6/tcp/4012/p2p/Qmcfuo1TM9uUiJp6dTbm915Rf1aTqm3a3dnmCdDQLHgvL5
  "QmcfV2sg9zaq7UUHVCGuSvT2M2rnLBAPsiE79vVyK3Cuev"  # /ip6/2606:4700:60::6/tcp/4013/p2p/QmcfV2sg9zaq7UUHVCGuSvT2M2rnLBAPsiE79vVyK3Cuev
)


gateway_peer_ids <- c(
  ########################################
  # IDs obtained from 2022-02 daily gateway probing
  # "astyanax.io"
  "12D3KooWAuSWjHi7cuYHqWAKqBQXTk7Tb1TAuEcuQdmbMuWgsbwg",
  
  # "gateway.ipfs.io"
  "12D3KooWF7jjTaY9zcYaq3YoNyPGshQZAGwmsYUdZ3yLo2xS6zs1",
  "12D3KooWG8THpfuHW9cj2m9nbrJJHeHy2asZnoEtYRxshhGdadMG",
  "12D3KooWKBRcG1YF58DdbEkcmXi2gEPJL3hCD83Jz1VVYhB9Dajc",
  "12D3KooWLaHEAVfncfKmPBpF1CYEazxA2ShmnoyK68G1zxV3K2oH",
  
  # "ipfs.infura.io"
  "12D3KooWA9F1NQ7x1aWfiCVx9bTfpu9GDgVb1PeLuTXV77rapnsj",
  "12D3KooWAvsEEhtTxrkoaBEEZDWzAMUJWJPNv3ryGHvEqzsrmoWg",
  "12D3KooWC9GUx1ZduRfbmxcdtMDMDZZtBdRHCp7eFM3xLyH3Egs7",
  "12D3KooWCL5hFsFmXcaxYB7RiCJAGH3UgpmJR9pFPKCvyed3RkWL",
  "12D3KooWCcFgyst5HgVsKG2Ni2MP42Gy2GVkT7o6FoydBW5Naapj",
  "12D3KooWEBxewRW3zeJEJ3tFmuhbJFdk2V6VrcMvCtbijTRPFcBX",
  "12D3KooWNVLY3xex7WsoRuTeE1TRjYh8c1FKyrX7VN3Bk4Lkq7wz",
  "12D3KooWNsKy2e6BVryinuhHPuBFw8QRAxjGrj7ApReCGaQSLpwe",
  "12D3KooWQsk7u372hk5y2GYRpZxzYYgGXGgMRvGymbio44S2HGUj",
  "12D3KooWR8rmGo7mJfSzuqWurE2ix5er1L5N92cuDuEKUzhJqEsq",
  "12D3KooWSV63NK2vo5cvEmT4R6AkgMoVKxQtQ4V2oVk3BnK1F9YX",
  
  # "183.252.17.149"
  "QmdZe2ezKGVYkydd3goxr8SEmxrNxUKPgEtxYmHhH3Vr1H",
  
  # "cf-ipfs.com"
  "QmcFf2FH3CEgTNHeMRGhN7HNHU1EXAxoEk6EFuSyXCsvRE",
  "QmcfJeB3Js1FG7T8YaZATEiaHqNKVdQfybYYkbT1knUswx",
  "QmcfR3V5YAtHBzxVACWCzXTt26SyEkxdwhGJ6875A8BuWx",
  "QmcfZD3VKrUxyP9BbyUnZDpbqDnT7cQ4WjPP8TRLXaoE7G",
  "QmcfZP2LuW4jxviTeG8fi28qjnZScACb8PEgHAc17ZEri3",
  "QmcfgsJsMtx6qJb74akCw1M24X1zFwgGo11h1cuhwQjtJP",
  "Qmcfr2FC7pFzJbTSDfYaSy1J8Uuy8ccGLeLyqJCKJvTHMi",
  "Qmcfuo1TM9uUiJp6dTbm915Rf1aTqm3a3dnmCdDQLHgvL5",
  
  # "infura-ipfs.io"
  "12D3KooWAvsEEhtTxrkoaBEEZDWzAMUJWJPNv3ryGHvEqzsrmoWg",
  "12D3KooWB7LPTnH7kDjuDYF6A81RUQh3NXAN9tpFi2NzunVc3KC1",
  "12D3KooWCL5hFsFmXcaxYB7RiCJAGH3UgpmJR9pFPKCvyed3RkWL",
  "12D3KooWCcFgyst5HgVsKG2Ni2MP42Gy2GVkT7o6FoydBW5Naapj",
  "12D3KooWDC7bpv518mttU5rtwoPZSm98G3YL4ayQbTfG3k4trc1t",
  "12D3KooWEBxewRW3zeJEJ3tFmuhbJFdk2V6VrcMvCtbijTRPFcBX",
  "12D3KooWEHvD44L83tKdexfwVhc2NMHHkqsfA1qXyQSh4e86dfUQ",
  "12D3KooWNVLY3xex7WsoRuTeE1TRjYh8c1FKyrX7VN3Bk4Lkq7wz",
  "12D3KooWNsKy2e6BVryinuhHPuBFw8QRAxjGrj7ApReCGaQSLpwe",
  "12D3KooWR8rmGo7mJfSzuqWurE2ix5er1L5N92cuDuEKUzhJqEsq",
  "12D3KooWSV63NK2vo5cvEmT4R6AkgMoVKxQtQ4V2oVk3BnK1F9YX",
  "12D3KooWSudCduvgxeon98JDoJxVXj3S5sB9hkzq5StmbjsJGAZe",
  
  # "ipfs.best-practice.se"
  "12D3KooWH7y46FhGaqtS47MFZXN6WhQaXgKWKTSCP3ASRRP5tGfm",
  "12D3KooWK37AjHAnsbEPCdt5zpY171LgpstC5EkegsCbpwW6L7QZ",
  "12D3KooWNybYMN9JBAspGKeanZNTWUsnjaZc6ZUz55YZbgykwXwQ",
  
  # "ipfs.azurewebsites.net"
  "12D3KooWJMbxhFUPdX4NWhJqBLb2wqi2szoYHkNZP923oRybHNwS",
  "12D3KooWKyhuZ7jfQBE9DKxzr7xFaASMRFPGWQYKuKZDVpsfMdxD",
  
  # "ipfs.fleek.co"
  "QmcFf2FH3CEgTNHeMRGhN7HNHU1EXAxoEk6EFuSyXCsvRE",
  "QmcFmLd5ySfk2WZuJ1mfSWLDjdmHZq7rSAua4GoeSQfs1z",
  "QmcfJeB3Js1FG7T8YaZATEiaHqNKVdQfybYYkbT1knUswx",
  "QmcfV2sg9zaq7UUHVCGuSvT2M2rnLBAPsiE79vVyK3Cuev",
  "QmcfVvzK4tMdFmpJjEKDUoqRgP4W9FnmJoziYX5GXJJ8eZ",
  "QmcfZP2LuW4jxviTeG8fi28qjnZScACb8PEgHAc17ZEri3",
  "QmcfgsJsMtx6qJb74akCw1M24X1zFwgGo11h1cuhwQjtJP",
  "Qmcfr2FC7pFzJbTSDfYaSy1J8Uuy8ccGLeLyqJCKJvTHMi",
  
  # "ipfs.genenetwork.org"
  "QmTx5i1AzqrNqWpDqHArJLtUrzsM46eY4XGD1en1MdAVzf",
  
  # "crustwebsites.net"
  "12D3KooWP7r9RR2Z4XymZ3fzAYR45MZBw3Quep5eoVvxgc5gSVGQ",
  
  # "ipfs.adatools.io"
  "12D3KooWJrJebfj1AhUgCvRDuXA5zgXBX7mT1GxXVAGVDxP8yJrC",
  
  # "ipfs.overpi.com"
  "12D3KooWJYtCPNYxn2a8Q3FGRckEFGMCzHH4BXmBy4YpVixeYvwj",
  
  # "ipfs.anonymize.com"
  "12D3KooWBP5Yi9v1swwG7cMvfByx5Q5U5ZwaANSvybZ9SciMUpv3",
  "12D3KooWF7jjTaY9zcYaq3YoNyPGshQZAGwmsYUdZ3yLo2xS6zs1",
  "12D3KooWG8THpfuHW9cj2m9nbrJJHeHy2asZnoEtYRxshhGdadMG",
  "12D3KooWKBRcG1YF58DdbEkcmXi2gEPJL3hCD83Jz1VVYhB9Dajc",
  "12D3KooWLaHEAVfncfKmPBpF1CYEazxA2ShmnoyK68G1zxV3K2oH",
  "12D3KooWMPAAKf7m9DWXCSepBS9gY6oTmeHw18PKDJMYuresYNhf",
  
  # "ipfs.io"
  "12D3KooWF7jjTaY9zcYaq3YoNyPGshQZAGwmsYUdZ3yLo2xS6zs1",
  "12D3KooWG8THpfuHW9cj2m9nbrJJHeHy2asZnoEtYRxshhGdadMG",
  "12D3KooWKBRcG1YF58DdbEkcmXi2gEPJL3hCD83Jz1VVYhB9Dajc",
  "12D3KooWLaHEAVfncfKmPBpF1CYEazxA2ShmnoyK68G1zxV3K2oH",
  
  # "hub.textile.io"
  "QmR69wtWUMm1TWnmuD4JqC1TWLZcc8iR2KrTenfZZbiztd",
  
  # "10.via0.com"
  "12D3KooWBP5Yi9v1swwG7cMvfByx5Q5U5ZwaANSvybZ9SciMUpv3",
  "12D3KooWF7jjTaY9zcYaq3YoNyPGshQZAGwmsYUdZ3yLo2xS6zs1",
  "12D3KooWG8THpfuHW9cj2m9nbrJJHeHy2asZnoEtYRxshhGdadMG",
  "12D3KooWKBRcG1YF58DdbEkcmXi2gEPJL3hCD83Jz1VVYhB9Dajc",
  "12D3KooWLaHEAVfncfKmPBpF1CYEazxA2ShmnoyK68G1zxV3K2oH",
  "12D3KooWMPAAKf7m9DWXCSepBS9gY6oTmeHw18PKDJMYuresYNhf",
  
  # "cloudflare-ipfs.com"
  "QmcFf2FH3CEgTNHeMRGhN7HNHU1EXAxoEk6EFuSyXCsvRE",
  "QmcFmLd5ySfk2WZuJ1mfSWLDjdmHZq7rSAua4GoeSQfs1z",
  "QmcfJeB3Js1FG7T8YaZATEiaHqNKVdQfybYYkbT1knUswx",
  "QmcfR3V5YAtHBzxVACWCzXTt26SyEkxdwhGJ6875A8BuWx",
  "QmcfV2sg9zaq7UUHVCGuSvT2M2rnLBAPsiE79vVyK3Cuev",
  "QmcfZD3VKrUxyP9BbyUnZDpbqDnT7cQ4WjPP8TRLXaoE7G",
  "QmcfZP2LuW4jxviTeG8fi28qjnZScACb8PEgHAc17ZEri3",
  "QmcfgsJsMtx6qJb74akCw1M24X1zFwgGo11h1cuhwQjtJP",
  "Qmcfr2FC7pFzJbTSDfYaSy1J8Uuy8ccGLeLyqJCKJvTHMi",
  "Qmcfuo1TM9uUiJp6dTbm915Rf1aTqm3a3dnmCdDQLHgvL5",
  
  # "ravencoinipfs-gateway.com"
  "12D3KooWAw2gTLa2LXhnx6tkhJcdPvvaYH82R6m8dxttDCPEkCtm",
  "12D3KooWHGJ8BrFuP3E7VveppKFPeUodAW5FtUHeJGkyVXR3GoWs",
  "12D3KooWS1ehpyirKUJma8tkVaMennGTCG9E822CXuWXnJ37YBqL",
  "12D3KooWSxmtT3azrbtDnadxC196HS6QoU2dNJvvyzAFGfkZPiPB",
  
  # "dweb.link"
  "12D3KooWF7jjTaY9zcYaq3YoNyPGshQZAGwmsYUdZ3yLo2xS6zs1",
  "12D3KooWG8THpfuHW9cj2m9nbrJJHeHy2asZnoEtYRxshhGdadMG",
  "12D3KooWKBRcG1YF58DdbEkcmXi2gEPJL3hCD83Jz1VVYhB9Dajc",
  "12D3KooWLaHEAVfncfKmPBpF1CYEazxA2ShmnoyK68G1zxV3K2oH",
  
  # "ipfs.eth.aragon.network"
  "12D3KooWBxWX59Kt4diGwBTAL2M4vr3FsPFVT9mqX8Qybk5hwvFH",
  "12D3KooWC6DS4xB5nkXZQ4pTWBJkuXC6EH51UX6mSbxKfdzY8vAZ",
  "12D3KooWDJ8iby2f4iYMzLKjyzeV1JLqYa56h7Lnoy6BUEhyT3eg",
  "12D3KooWFbvyymazLynU4qjznQZWkjKHPCDVQRkgAjwMWgJ8qgm2",
  "12D3KooWLRtcKkxw2WBdDn5MiXiwm6AsVMvE6ndW77Evwwy99Hss",
  "12D3KooWN4Tip3Tgkmx1AMpKGVqdWmESyLC8Ld37nTsuhJdWzJP3",
  "12D3KooWP7Ebjne4H4HYnvwaczjCY2dYo5z3fRSFy7ZqnKb2dDDg",
  
  # "robotizing.net"
  "12D3KooWBP5Yi9v1swwG7cMvfByx5Q5U5ZwaANSvybZ9SciMUpv3",
  "12D3KooWF7jjTaY9zcYaq3YoNyPGshQZAGwmsYUdZ3yLo2xS6zs1",
  "12D3KooWG8THpfuHW9cj2m9nbrJJHeHy2asZnoEtYRxshhGdadMG",
  "12D3KooWKBRcG1YF58DdbEkcmXi2gEPJL3hCD83Jz1VVYhB9Dajc",
  "12D3KooWLaHEAVfncfKmPBpF1CYEazxA2ShmnoyK68G1zxV3K2oH",
  
  # "gateway.pinata.cloud"
  "QmSRg4CQN4aSTKDahNSjwE2BnMRjZkthS5mdmcnau85FM5",
  
  # "video.oneloveipfs.com"
  "QmeC7HMgsc2ohph1P31ArDtxcUo669ykAuxx8j2TQ99esS",
  
  # "ipfs.tubby.cloud"
  "12D3KooWRi8xBxdKNWtrB2TF3xTD4xfHTjgUCixSy8R5tg2eXVpj",
  
  ########################################
  # Old (pre 2022) peer IDs of various origins
  "12D3KooWAuMTLB3XF8MJKRLqNxZF5cTjHP29LrhCzsr5rAqvRXHG",
  "12D3KooWAuSWjHi7cuYHqWAKqBQXTk7Tb1TAuEcuQdmbMuWgsbwg",
  "12D3KooWAw2gTLa2LXhnx6tkhJcdPvvaYH82R6m8dxttDCPEkCtm",
  "12D3KooWC85Q7yAGg9nYydXtxMn7wJYEEqLYDivGqR8diEatDqKR",
  "12D3KooWCLcPyc3R1NbX2xiXBzAKPqCNvZaq5sPNfqJsyndEFLoh",
  "12D3KooWDSRkVvbwoyUz73RKpmbag9NGiQYck2ECR48f6qPyDAf5",
  "12D3KooWDazxTi393rscxmbE6pa519odejM54Uiftu8C1Ui3A6ZL",
  "12D3KooWEFZPSv3Q4fqBgVUjQDLY47VKncqLT9Lsfmn6F76ujyDk",
  "12D3KooWEQfhhJBhvAK9hsoyqydnZKXKTSzeowacpHSWFJctQzCM",
  "12D3KooWEfQkss14RPkFoGCby5ZRo5F2UYEMEfSAbYUdjeAE6Dk3",
  "12D3KooWF3FyhbgbtNvGgRsr578XMRU9bPhkfmphfFFuwNQHxDij",
  "12D3KooWFL9eAVCExTrTkEEubJiDA8ZniFHh7UndS1deaR5opuCY",
  "12D3KooWGR6Ri46PESKzRjku5nRe4GMyHgLq5obfQPxJFDJrQQBq",
  "12D3KooWGchmAQ5NMwEV3UAgEv7xwEwmL76sUZmSdPxsqfXigXKc",
  "12D3KooWH5uXtcGrMvTgJt1FMyfxgPm3TzugDMCsnUaCkRZ2vE9H",
  "12D3KooWJSmjXFjzNxYhs2N9ijgZHyrCDanysUpqDpR8NbYh6oE5",
  "12D3KooWJYtCPNYxn2a8Q3FGRckEFGMCzHH4BXmBy4YpVixeYvwj",
  "12D3KooWKLDNrSdoZHrXqavrxy35EsbP456spzc2mcB5dPFWTFNN",
  "12D3KooWKXvRXzNLAB58AwFNDb5jtj1QmJ58sitywt8vk18aNQhG",
  "12D3KooWLRtcKkxw2WBdDn5MiXiwm6AsVMvE6ndW77Evwwy99Hss",
  "12D3KooWLxk54fH6RhRxnaiQXTVwJiNmXUb4ZULGgJ4rj6mL6kWe",
  "12D3KooWMLtu3emAahojNa4C7CHZShvQvafMcYHtFupb7Ep5LfK3",
  "12D3KooWMUx7JwkThMdfbqSBh96GiBkhrGuosxjXAcgATJUZbKFy",
  "12D3KooWNCtF2LFA9s5bzcQzkhg4MSgVbSAxS63B5Z17fQtjhDep",
  "12D3KooWNNKdz6DehH3epj7CBuMk5d1r778EtpUJTnLYdoVBFWYH",
  "12D3KooWNQNFmmGBf1YFrbrcdsnxPuQTP8W2njEJEqyeBtpjJxA1",
  "12D3KooWNwE1NC4T5yZBszAeDs8JM5R79wKLQVhpetsquZtfqyp4",
  "12D3KooWQABZX4kmEQTV2XXBTFvDK1Yrdz8DhJTLctyqnqr7QsHo",
  "12D3KooWQgbbyJy4oWRJkA5qjc2qb5p82KCrKj7VVEe4uPSCu5vB",
  "12D3KooWQr924mGoCDe7MHpMBv6k7xjWkvhTcxmQNvXrRVMKNZBQ",
  "12D3KooWRTRb7Zcz53rL7GvhdASjMLeEU8GsqyjvNcLD58u7Bvtr",
  "12D3KooWRi8xBxdKNWtrB2TF3xTD4xfHTjgUCixSy8R5tg2eXVpj",
  "QmNXQUQipLr9FRWXWwkKakc6sNM1YhwvXFjEhyDub1fbaR",
  "QmNfngzqenaE1Ru3T6i4ic4owa92py1XXVFkxBimumLrK7",
  "QmNjm16wkUUsUoRmr3b8QoQAPZYBwfBHcygkjGbSauTSWu",
  "QmPCYe2TnxuTweNjfoe9181U1e8oEfh44kHu2odetS1ZQf",
  "QmPcDM7QGkQZGc2EV12CiG6STbFF7G5w4gsFv4v4m48o6k",
  "QmPvnFXWAz1eSghXD6JKpHxaGjbVo4VhBXY2wdBxKPbne5",
  "QmQPF396xeGSnrcZSSqSVJGigerAoCmnnAyzu8E8zGxQ7D",
  "QmQSsR94ZZHzws1UWzGgrD7HPuW3nqhDRoaYNGrUgvD9d1",
  "QmR69wtWUMm1TWnmuD4JqC1TWLZcc8iR2KrTenfZZbiztd",
  "QmRUd3KRLGW8UMc1AmnzMqX7YdSjR6pTsRwFoJiqeheZoJ",
  "QmRzfM4kLwk6AjnZVmFra9RdMguWyjU4j8tctNz6dzxjxc",
  "QmSPz3WfZ1xCq6PCFQj3xFHAPBRUudbogcDPSMtwkQzxGC",
  "QmSRg4CQN4aSTKDahNSjwE2BnMRjZkthS5mdmcnau85FM5",
  "QmSUobMzQkFHYty1ABu7B6MtbYaDCK9APm4irtgg96xrhm",
  "QmSfPx5uNa1Jh4Rn7LTtV6uaiVoWP7MjvMdzAKEM2R9swJ",
  "QmTUUXR3Trt3cVKC6owEbTZMGRNGfMyYuLjhm9uo2tpaHf",
  "QmTiXNcqSSV9inzKXzUbVJ4mc8qov1yXqTEWjAemdbLmVS",
  "QmTwCywSi2DLBfRDo1UhqKSPiWkBnD2GdmgdFCSHVPiEEn",
  "QmTx5i1AzqrNqWpDqHArJLtUrzsM46eY4XGD1en1MdAVzf",
  "QmUAzL9Fpp1fsZnrkANcitMevCoTWKP3TJcgTuc2D3b7vi",
  "QmVBxJ5GekATHi89H8jbXjaU6CosCnteomjNR5xar2aH3q",
  "QmVLEz2SxoNiFnuyLpbXsH6SvjPTrHNMU88vCQZyhgBzgw",
  "QmVLb8bi8oAmLwrTdjGurtVGN7FaJvhXP5UM3s56uyqfxL",
  "QmVjLyXj8NtBUt1XoEWLmwwb9NPZJrRUsjppi8a3uw29CZ",
  "QmVk8DsWXJEKQDavJrkKPgkngmwJpVUqt7fieZkQBQtaa7",
  "QmWJWxD31AhSPEFHNe9dCMxwxHv5BysUDdizCreupHKTBn",
  "QmXAdzKCUZTg9SLLRK6nrfyhJmK1S43n2y5EBeGQ7zqhfW",
  "QmXUEWEWWq6dhKqBwryPSR8jYoa9ByeAaqnz62CRDY7ppL",
  "QmXow5Vu8YXqvabkptQ7HddvNPpbLhXzmmU53yPCM54EQa",
  "QmYAXDCy9sZCEZFmQtFi6f5a7JbBoWJsixdHzrKuNPxumv",
  "QmYGUewY6m8FkNk85nVec1CQ69B5BpesKgFuaqqsbBx1Sv",
  "QmYX9XFBiQcjep2XacPXYfJ5pYcQavLnTwzd76KuL7edgV",
  "QmYZmw1PQXtKVeAjRfn8TnKYAurSdezRd1kdDjenzxTF6y",
  "QmYybkVQWunfk1jTrSF4ikiR9RawiJx34MguRv7zrYKgwD",
  "QmZ86ow1byeyhNRJEatWxGPJKcnQKG7s51MtbHdxxUddTH",
  "QmZBKtbf3mtUFMgpJ9eQixGu6shZp7sNN2CfeaBRnwgcJX",
  "Qmb53qh7fo26k99u3KATf4nFDUbg2tnUPdfhAqp63jU67S",
  "Qmb5M5XCnYZ7XzARYvdj5qbZGe3JPeYwa9tp7Um7CZLyCQ",
  "QmbUa4f2y1yqAwFBPS61A818xFoqVAPbbJPXti5wjV3Tdd",
  "QmbcQjYkgiPAFUk9Y1E8cHH8SebwxWCD9omdEMQdHWa5CP",
  "QmcF5yUHqZYWDZYEdZRK8XALRtQxNqJ5w2jPecGDK4jUxZ",
  "QmcFf2FH3CEgTNHeMRGhN7HNHU1EXAxoEk6EFuSyXCsvRE",
  "QmcFmLd5ySfk2WZuJ1mfSWLDjdmHZq7rSAua4GoeSQfs1z",
  "QmcZnF9i1TTwoDBwAHjtv4spHGpWbEQDy8QqaXhBqtQEoU",
  "QmcfFmzSDVbwexQ9Au2pt5YEXHK5xajwgaU6PpkbLWerMa",
  "QmcfJeB3Js1FG7T8YaZATEiaHqNKVdQfybYYkbT1knUswx",
  "QmcfR3V5YAtHBzxVACWCzXTt26SyEkxdwhGJ6875A8BuWx",
  "QmcfV2sg9zaq7UUHVCGuSvT2M2rnLBAPsiE79vVyK3Cuev",
  "QmcfVvzK4tMdFmpJjEKDUoqRgP4W9FnmJoziYX5GXJJ8eZ",
  "QmcfZD3VKrUxyP9BbyUnZDpbqDnT7cQ4WjPP8TRLXaoE7G",
  "QmcfZP2LuW4jxviTeG8fi28qjnZScACb8PEgHAc17ZEri3",
  "QmcfgsJsMtx6qJb74akCw1M24X1zFwgGo11h1cuhwQjtJP",
  "Qmcfr2FC7pFzJbTSDfYaSy1J8Uuy8ccGLeLyqJCKJvTHMi",
  "Qmcfuo1TM9uUiJp6dTbm915Rf1aTqm3a3dnmCdDQLHgvL5",
  "QmcwZMGdokR9fTF1ULQNdUDYVgtNiHD6eXHizxYxJZ6aCz",
  "QmcywxEW3uxg3MQWrfWywVSS7YuhMvYuzBLQxvdGcjGzSS",
  "QmdAXatABqmgZGZ8LJMMaCT3esQvpTPPqH3Pk9ZjDDA3JW",
  "QmeC7HMgsc2ohph1P31ArDtxcUo669ykAuxx8j2TQ99esS",
  "QmeLsyBuzK6ehenYBRazX28xEfhFicCk52zuWMtf6xUFfv",
  "QmeQUNkPJanr64pL9fkDHfBjbJxBkZZKwhnez3aQFZCkj4",
  "QmfTK9b4ShVjZjL8tvy9FfQbEvxZtSAH1SWR1xAQxFKz6w"
)

CSV_MESSAGE_TYPE_INCREMENTAL <- 1;
CSV_MESSAGE_TYPE_FULL <- 2;
CSV_MESSAGE_TYPE_SYNTHETIC <- 3;
CSV_MESSAGE_TYPE_LEVELS <- c(CSV_MESSAGE_TYPE_INCREMENTAL,
                             CSV_MESSAGE_TYPE_FULL,
                             CSV_MESSAGE_TYPE_SYNTHETIC)
CSV_MESSAGE_TYPE_LABELS <- c("INC",
                             "FULL",
                             "SYNTH")

CSV_CONNECTION_EVENT_CONNECTED_FOUND <- 1;
CSV_CONNECTION_EVENT_CONNECTED_NOT_FOUND <- 2;
CSV_CONNECTION_EVENT_DISCONNECTED_FOUND <- 3;
CSV_CONNECTION_EVENT_DISCONNECTED_NOT_FOUND <- 4;
CSV_CONNECTION_EVENT_CONNECTED = c("CONNECTED; FOUND", "CONNECTED; NOT_FOUND");
CSV_CONNECTION_EVENT_DISCONNECTED = c("DISCONNECTED; FOUND", "DISCONNECTED; NOT_FOUND");
CSV_CONNECTION_EVENT_LEVELS <- c(CSV_CONNECTION_EVENT_CONNECTED_FOUND,
                                 CSV_CONNECTION_EVENT_CONNECTED_NOT_FOUND,
                                 CSV_CONNECTION_EVENT_DISCONNECTED_FOUND,
                                 CSV_CONNECTION_EVENT_DISCONNECTED_NOT_FOUND)
CSV_CONNECTION_EVENT_LABELS <- c("CONNECTED; FOUND",
                                 "CONNECTED; NOT_FOUND",
                                 "DISCONNECTED; FOUND",
                                 "DISCONNECTED; NOT_FOUND")

CSV_ENTRY_TYPE_CANCEL<- 1;
CSV_ENTRY_TYPE_CANCEL_LABEL <- "CANCEL";
CSV_ENTRY_TYPE_WANT_BLOCK<- 2;
CSV_ENTRY_TYPE_WANT_BLOCK_LABEL <- "WANT_BLOCK";
CSV_ENTRY_TYPE_WANT_BLOCK_SEND_DONT_HAVE<- 3;
CSV_ENTRY_TYPE_WANT_BLOCK_SEND_DONT_HAVE_LABEL<- "WANT_BLOCK; SEND_DH" ;
CSV_ENTRY_TYPE_WANT_HAVE<- 4;
CSV_ENTRY_TYPE_WANT_HAVE_SEND_DONT_HAVE<- 5;
CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_FULL_WANTLIST <- 6;
CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_DISCONNECT <- 7;
CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_END_OF_SIMULATION <- 8;
CSV_ENTRY_TYPE_REQUEST = c("WANT_BLOCK","WANT_HAVE","WANT_BLOCK; SEND_DH","WANT_HAVE; SEND_DH")
CSV_ENTRY_TYPE_SYNTHETIC_CANCEL = c("SYNTH_CANCEL_FULL_WL","SYNTH_CANCEL_DISCONNECT","SYNTH_CANCEL_END_OF_SIM")
CSV_ENTRY_TYPE_LEVELS <- c(CSV_ENTRY_TYPE_CANCEL,
                           CSV_ENTRY_TYPE_WANT_BLOCK,
                           CSV_ENTRY_TYPE_WANT_BLOCK_SEND_DONT_HAVE,
                           CSV_ENTRY_TYPE_WANT_HAVE,
                           CSV_ENTRY_TYPE_WANT_HAVE_SEND_DONT_HAVE,
                           CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_FULL_WANTLIST,
                           CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_DISCONNECT,
                           CSV_ENTRY_TYPE_SYNTHETIC_CANCEL_END_OF_SIMULATION)
CSV_ENTRY_TYPE_LABELS <- c(CSV_ENTRY_TYPE_CANCEL_LABEL,
                           CSV_ENTRY_TYPE_WANT_BLOCK_LABEL,
                           CSV_ENTRY_TYPE_WANT_BLOCK_SEND_DONT_HAVE_LABEL,
                           "WANT_HAVE",
                           "WANT_HAVE; SEND_DH",
                           "SYNTH_CANCEL_FULL_WL",
                           "SYNTH_CANCEL_DISCONNECT",
                           "SYNTH_CANCEL_END_OF_SIM")

CSV_DUPLICATE_STATUS_NO_DUP <- 0;
CSV_DUPLICATE_STATUS_DUP_FULL_WANTLIST <- 1;
CSV_DUPLICATE_STATUS_DUP_RECONNECT <- 2;
CSV_DUPLICATE_STATUS_DUP_SLIDING_WINDOW <- 4;
CSV_DUPLICATE_STATUS_LEVELS <- c(CSV_DUPLICATE_STATUS_NO_DUP,
                                 CSV_DUPLICATE_STATUS_DUP_FULL_WANTLIST,
                                 CSV_DUPLICATE_STATUS_DUP_RECONNECT,
                                 CSV_DUPLICATE_STATUS_DUP_RECONNECT + CSV_DUPLICATE_STATUS_DUP_FULL_WANTLIST ,
                                 CSV_DUPLICATE_STATUS_DUP_SLIDING_WINDOW,
                                 CSV_DUPLICATE_STATUS_DUP_SLIDING_WINDOW + CSV_DUPLICATE_STATUS_DUP_FULL_WANTLIST,
                                 CSV_DUPLICATE_STATUS_DUP_SLIDING_WINDOW + CSV_DUPLICATE_STATUS_DUP_RECONNECT,
                                 CSV_DUPLICATE_STATUS_DUP_SLIDING_WINDOW + CSV_DUPLICATE_STATUS_DUP_RECONNECT + CSV_DUPLICATE_STATUS_DUP_FULL_WANTLIST)
CSV_DUPLICATE_STATUS_LABELS <- c("NO_DUP",
                                 "FULL_WL",
                                 "RECONNECT",
                                 "RECONNECT; FULL_WL",
                                 "SLIDING_WIN",
                                 "SLIDING_WIN; FULL_WL",
                                 "SLIDING_WIN; RECONNECT",
                                 "SLIDING_WIN; RECONNECT; FULL_WL")
