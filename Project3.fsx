#r "nuget: Akka.FSharp"

open Akka.FSharp
open Akka.Actor
open System
open System.Security.Cryptography

type Message = 
    | AssignSuccessor of IActorRef
    | AssignPredecessor of IActorRef
    | Stabilize
    | Join of IActorRef
    | FindSuccessor of IActorRef*int
    | PrintChord of IActorRef
    | PopulateFingerTable
    | ReturnAverage
    | IncreaseCounter of int
  
    

let system = System.create "chord-system" (Configuration.defaultConfig())
printfn "%A" (Environment.GetCommandLineArgs())

let numNodes = Environment.GetCommandLineArgs().[2] |> int
let numRequests = Environment.GetCommandLineArgs().[3] |> int
let chordSize = numNodes * 16
let mutable chord : IActorRef[] = [|for i in 0 .. chordSize -> null|]
let m = Math.Log(chordSize |> float, 2.0) |> ceil |> int

let getSHA1Encoding(str: string) = 
    System.Text.Encoding.ASCII.GetBytes(str) 
    |> (SHA1.Create()).ComputeHash
    |> Array.map (fun (x : byte) -> System.String.Format("{0:x2}", x))
    |> String.concat String.Empty

let getHashedIndex(num) =
    getSHA1Encoding(string num)
    |> Seq.toList
    |> Seq.indexed
    |> Seq.sumBy (fun (i,x)-> (int64 x)*int64(2.0**(i|>float)))

let hash(node: IActorRef) = 
    (node.Path.Name |> getSHA1Encoding |> getHashedIndex)% (int64 chordSize) |> int

let Counter(mailbox: Actor<_>) = 
    let mutable totalHops = 0.0
    let mutable totalRequests = numRequests |> float

    let rec loop count = 
        actor {
            let! msg = mailbox.Receive()

            match msg with
            | IncreaseCounter(hops) -> totalHops <- totalHops + (hops |> float)
            | ReturnAverage -> printfn "Average Hops = %f" (totalHops/totalRequests)
                               system.Terminate() |> ignore
            | _ -> printfn "Wrong message"
            return! loop(count+1)
        }
    loop(0)

let counter = spawn system "counter" Counter

let Node(mailbox: Actor<_>) = 
    let mutable fingerTable : IActorRef list = []
    let mutable successor : IActorRef = null
    let mutable predecessor : IActorRef = null
        
    let rec loop count =
        actor {
            let! message = mailbox.Receive();
            
            
            match message with
            | AssignSuccessor(s) -> successor <- s
            | AssignPredecessor(p) -> predecessor <- p
            | Stabilize -> printfn "STABILIZATIONNNNNNNN"

            | PopulateFingerTable -> 
                                     let x = hash(mailbox.Self)
                                     let indexes = [for i in 1 .. m-1 -> 2.0 ** (i |> float)] |> List.map(fun x -> (int x)%chordSize)
                                     
                                     for i in indexes do
                                        let mutable j = (i + x)%chordSize
                                        while chord.[j] = null do
                                            j <- (j + 1)%chordSize
                                        
                                        fingerTable <- fingerTable @ [chord.[j]]

                                     fingerTable <- List.sortWith (fun a b -> compare (hash b) (hash a)) fingerTable
                                     

                                     

            | FindSuccessor(node,i) -> 
                                     
                                     let id = hash(node)
                                     if id >= hash(mailbox.Self) && id <= hash(successor) then
                                        printfn "Hops[%d]=%d" id i
                                        counter <! IncreaseCounter(i)
                                        
                                     else
                                        let index, jumpNode = fingerTable |> List.indexed |> List.find(fun (i,x) -> id >= hash(x) || i = fingerTable.Length-1)
                                        jumpNode <! FindSuccessor(node,i+1)                                                                                               
                                        
            | Join(node) -> let id = hash(node)
                            
                            if id > hash(mailbox.Self) && successor = null then
                                successor <- node
                                predecessor <- node
                                node <! AssignPredecessor(mailbox.Self)
                                node <! AssignSuccessor(mailbox.Self)

                            elif id > hash(mailbox.Self) && id <= hash(successor) then
                                node <! AssignSuccessor(successor)
                                node <! AssignPredecessor(mailbox.Self)
                                successor <! AssignPredecessor(node)
                                successor <- node

                            elif id > hash(mailbox.Self) && hash(successor) <= hash(mailbox.Self) then
                                node <! AssignSuccessor(successor)
                                node <! AssignPredecessor(mailbox.Self)
                                successor <! AssignPredecessor(node)
                                successor <- node

                            elif id > hash(mailbox.Self) && id > hash(successor) then
                                successor <! Join(node)

                            elif id < hash(mailbox.Self) then
                                node <! AssignSuccessor(mailbox.Self)
                                node <! AssignPredecessor(predecessor)
                                predecessor <! AssignSuccessor(node)
                                predecessor <- node

            | PrintChord(s) ->  printfn " %d %d %d" (hash(predecessor)) (hash(mailbox.Self)) (hash(successor))
            | _ -> ()

            return! loop (count+1)
        }
    loop(0)


let nodeRef1 = spawn system "N1" Node
chord.[hash(nodeRef1)] <- nodeRef1

let nodes = [for i=2 to numNodes do yield spawn system $"N{i}" Node]

nodes
|> List.iter(fun n -> chord.[hash(n)] <- n)



printfn "Joining nodes wait..."
nodes
|> List.iter (fun n ->  Async.Sleep(50) |> Async.RunSynchronously
                        nodeRef1 <! Join(n))
printfn "Joined all nodes"

Async.Sleep(1000) |> Async.RunSynchronously

nodeRef1 <! PopulateFingerTable
nodes |> List.iter(fun n -> 
                            n <! PopulateFingerTable)

let requestNodes = nodes |> List.filter ( fun n -> hash(n) > hash(nodeRef1))
let requestNodes2 = requestNodes.[0.. numRequests]

nodeRef1 <! FindSuccessor(requestNodes2.[0], 0)

requestNodes2 |> List.iter ( fun node -> Async.Sleep(500) |> Async.RunSynchronously
                                         nodeRef1 <! FindSuccessor(node, 0) )

Async.Sleep(2000) |> Async.RunSynchronously
printfn "Done with all nodes"

counter <! ReturnAverage

system.WhenTerminated.Wait()

