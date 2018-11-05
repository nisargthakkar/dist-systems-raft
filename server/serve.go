package main

import (
	"fmt"
	"log"
	"math"
	rand "math/rand"
	"net"
	"sort"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-nisargthakkar/pb"
)

const TIME_MULTIPLIER = 1
const LOG_INDEXING int64 = 1 // TODO: Raft log Index and log term are 0 indexed or 1 indexed
const HEARTBEAT_TIMEOUT = 200 * TIME_MULTIPLIER * time.Millisecond

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput
}

// The struct to respond to AppendEntries
type AppendResponse struct {
	ret          *pb.AppendEntriesRet
	err          error
	peer         string
	messageIndex int64
}

// The struct to respond to RequestVote
type VoteResponse struct {
	ret  *pb.RequestVoteRet
	err  error
	peer string
	term int64
}

func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	// Constant
	const DurationMax = 4000 * TIME_MULTIPLIER
	const DurationMin = 1000 * TIME_MULTIPLIER
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r))
}

// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

func getAppendEntriesArgs(nextIndex int64, commandLog []*pb.Entry, currentTerm int64, id string, commitIndex int64) *pb.AppendEntriesArgs {
	prevLogIndex := nextIndex - 1
	prevLogTerm := int64(-1 + LOG_INDEXING)

	if prevLogIndex >= LOG_INDEXING {
		prevLogTerm = commandLog[prevLogIndex-LOG_INDEXING].Term
	}

	sendEntries := commandLog[prevLogIndex-LOG_INDEXING+1:]
	return &pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, LeaderCommit: commitIndex, Entries: sendEntries}
}

func getMajorityAgreementIndex(matchIndex *map[string]int64, lastLogIndex int64) int64 {
	values := []int64{lastLogIndex}
	for _, value := range *matchIndex {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] > values[j] }) // Descending order

	return values[len(values)/2]
}

func appendEntriesForClient(nextIndex int64, commandLog []*pb.Entry, currentTerm int64, id string, commitIndex int64, raftClient pb.RaftClient, peer string, appendResponseChan chan AppendResponse) {
	appendEntriesArgs := getAppendEntriesArgs(nextIndex, commandLog, currentTerm, id, commitIndex)

	appendEntriesArgsVal := *appendEntriesArgs
	prevLogIndex := appendEntriesArgsVal.PrevLogIndex
	sendEntries := appendEntriesArgsVal.Entries

	// Send in parallel so we don't wait for each client.
	go func(c pb.RaftClient, p string) {
		ret, err := c.AppendEntries(context.Background(), appendEntriesArgs)
		appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, messageIndex: prevLogIndex + int64(len(sendEntries))}
	}(raftClient, peer)
}

func beginRaft(currentTerm int64, commandLog []*pb.Entry, responseLog []InputChannelType, op InputChannelType, nextIndex map[string]int64, id string, commitIndex int64, peerClients *map[string]pb.RaftClient, appendResponseChan chan AppendResponse) ([]*pb.Entry, []InputChannelType) {
	entry := &pb.Entry{Term: currentTerm, Index: int64(len(commandLog)) - LOG_INDEXING + 1 + 1, Cmd: &op.command}

	// Adding the command to the leader's log
	commandLog = append(commandLog, entry)
	responseLog = append(responseLog, op)

	for p, c := range *peerClients {
		appendEntriesForClient(nextIndex[p], commandLog, currentTerm, id, commitIndex, c, p, appendResponseChan)
	}

	return commandLog, responseLog
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}
	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}

	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)

	const (
		FOLLOWER  = iota
		CANDIDATE = iota
		LEADER    = iota
	)

	commandLog := make([]*pb.Entry, 0)
	responseLog := make([]InputChannelType, 0)

	dummyResponseChannel := make(chan pb.Result, 1)

	var currentTerm int64 = 0
	votedFor := ""
	currentLeader := ""
	commitIndex := -1 + LOG_INDEXING
	lastApplied := -1 + LOG_INDEXING
	currentRole := FOLLOWER

	nextIndex := make(map[string]int64)
	matchIndex := make(map[string]int64)

	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r))

	// Create a timer and start running it
	heartbeatTimer := time.NewTimer(HEARTBEAT_TIMEOUT)

	voteGrants := make(map[string]bool)
	// Run forever handling inputs from various channels
	for {
		select {
		case <-timer.C:
			// The timer went off.
			// log.Printf("Election Timeout")

			if currentRole == LEADER {
				continue
			}

			currentRole = CANDIDATE
			currentTerm = currentTerm + 1
			voteGrants = make(map[string]bool)
			currentLeader = ""

			votedFor = id
			voteGrants[id] = true

			log.Printf("Requesting Votes for term: %d", currentTerm)

			prevLogIndex := int64(len(commandLog)) - 1 + LOG_INDEXING
			prevLogTerm := int64(-1 + LOG_INDEXING)

			if prevLogIndex >= LOG_INDEXING {
				prevLogTerm = commandLog[prevLogIndex-LOG_INDEXING].Term
			}

			for p, c := range peerClients {
				// Send in parallel so we don't wait for each client.
				go func(c pb.RaftClient, p string) {
					// If a node is removed from Candidate, it should not request any further votes
					if currentRole == CANDIDATE {
						ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: currentTerm, CandidateID: id, LastLogIndex: prevLogIndex, LasLogTerm: prevLogTerm})
						voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p, term: currentTerm}
					}
				}(c, p)
			}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r)
		case op := <-s.C:
			// We received an operation from a client
			if currentRole != LEADER {
				// TODO: redirect client to leader
				go func(op InputChannelType) {
					for currentLeader == "" {
					}
					if currentLeader == id {
						commandLog, responseLog = beginRaft(currentTerm, commandLog, responseLog, op, nextIndex, id, commitIndex, &peerClients, appendResponseChan)
						heartbeatTimer.Reset(HEARTBEAT_TIMEOUT)
					} else {
						op.response <- pb.Result{Result: &pb.Result_Redirect{&pb.Redirect{Server: currentLeader}}}
					}
				}(op)
				continue
			}
			// Use Raft to make sure it is safe to actually run the command.

			commandLog, responseLog = beginRaft(currentTerm, commandLog, responseLog, op, nextIndex, id, commitIndex, &peerClients, appendResponseChan)
			heartbeatTimer.Reset(HEARTBEAT_TIMEOUT)
		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			// log.Printf("Received append entry from %v", ae.arg.LeaderID)
			// Reply false if term < currentTerm
			if ae.arg.Term < currentTerm {
				// Reply false
				ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
			} else if ae.arg.PrevLogIndex-LOG_INDEXING != -1 && int64(len(commandLog)) <= ae.arg.PrevLogIndex-LOG_INDEXING {
				// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
				currentLeader = ae.arg.LeaderID
				currentTerm = ae.arg.Term
				currentRole = FOLLOWER
				votedFor = ""
				ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
			} else if ae.arg.PrevLogIndex-LOG_INDEXING != -1 && commandLog[ae.arg.PrevLogIndex-LOG_INDEXING].Term != ae.arg.PrevLogTerm {
				// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
				// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
				// TODO: Should we actually go and delete these? Possible optimization
				commandLog = commandLog[:ae.arg.PrevLogIndex-LOG_INDEXING]
				currentLeader = ae.arg.LeaderID
				currentTerm = ae.arg.Term
				currentRole = FOLLOWER
				votedFor = ""
				ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
			} else {
				// Append any new entries not already in the log
				// Empty all elements in commandLog after PrevLogIndex

				commandLog = commandLog[:ae.arg.PrevLogIndex-LOG_INDEXING+1]
				for i := 0; i < len(ae.arg.Entries); i = i + 1 {
					commandLog = append(commandLog, ae.arg.Entries[i])
				}

				// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
				if ae.arg.LeaderCommit > commitIndex {
					commitIndex = int64(math.Min(float64(ae.arg.LeaderCommit), float64(len(commandLog))-1+float64(LOG_INDEXING)))
					// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied]
					for commitIndex > lastApplied {
						lastApplied = lastApplied + 1
						log.Printf("Applied command to store Op: %v Args: %v", commandLog[lastApplied-LOG_INDEXING].Cmd.Operation, commandLog[lastApplied-LOG_INDEXING].Cmd.Arg)
						go s.HandleCommand(InputChannelType{command: *commandLog[lastApplied-LOG_INDEXING].Cmd, response: dummyResponseChannel})
					}
				}

				currentLeader = ae.arg.LeaderID
				currentTerm = ae.arg.Term
				ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: true}
			}

			if ae.arg.Term > currentTerm {
				currentLeader = ae.arg.LeaderID
				currentTerm = ae.arg.Term
				currentRole = FOLLOWER
				votedFor = ""
			}

			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r)
		case vr := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer
			// log.Printf("Received vote request from %v", vr.arg.CandidateID)
			if vr.arg.Term < currentTerm {
				// Reply false if term < currentTerm
				vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
			} else {
				// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
				if (votedFor == "" || votedFor == vr.arg.CandidateID) && vr.arg.LastLogIndex >= int64(len(commandLog))-1+LOG_INDEXING {
					currentTerm = vr.arg.Term
					votedFor = vr.arg.CandidateID
					currentRole = FOLLOWER
					vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: true}
					// This will also take care of any pesky timeouts that happened while processing the operation.
					restartTimer(timer, r)
				} else {
					vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
				}
			}
		case vr := <-voteResponseChan:
			// We received a response to a previous vote request.
			if vr.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				// log.Printf("Error calling RPC %v", vr.err)
			} else {
				// log.Printf("Got response to vote request from %v", vr.peer)
				// log.Printf("Peers %s granted %v term %v", vr.peer, vr.ret.VoteGranted, vr.ret.Term)
				if vr.ret.Term > currentTerm {
					voteGrants = make(map[string]bool)
					currentRole = FOLLOWER
					votedFor = ""
					currentTerm = vr.ret.Term
					currentLeader = ""
					// Restarting Timer because no longer a candidate
					restartTimer(timer, r)
				} else if currentRole != CANDIDATE {
					continue
				} else if vr.ret.VoteGranted == true && vr.term == currentTerm {
					voteGrants[vr.peer] = true
					if len(voteGrants) > (len(*peers)+1)/2 {
						log.Printf("Leader elected for term %v", currentTerm)
						currentRole = LEADER
						prevLogIndex := int64(len(commandLog)) - 1 + LOG_INDEXING
						currentLeader = id

						for _, p := range *peers {
							nextIndex[p] = prevLogIndex + 1
							matchIndex[p] = int64(-1 + LOG_INDEXING)
						}

						responseLog = make([]InputChannelType, 0)
						for _, logItem := range commandLog {
							responseLog = append(responseLog, InputChannelType{command: *logItem.Cmd, response: dummyResponseChannel})
						}

						// Send in parallel so we don't wait for each client.
						for p, c := range peerClients {
							appendEntriesForClient(nextIndex[p], commandLog, currentTerm, id, commitIndex, c, p, appendResponseChan)
						}

						heartbeatTimer.Reset(HEARTBEAT_TIMEOUT)
					}
				}
			}
		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			// log.Printf("Got append entries response from %v", ar.peer)

			if ar.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				// log.Printf("Error calling RPC %v", ar.err)
			} else if ar.ret.Term != currentTerm {
				currentRole = FOLLOWER
				currentTerm = ar.ret.Term
				votedFor = ""
				currentLeader = ""
				// Restarting Timer because no longer a leader
				restartTimer(timer, r)
			} else if currentRole != LEADER {
				continue
			} else if !ar.ret.Success {
				if nextIndex[ar.peer]-LOG_INDEXING > 0 {
					nextIndex[ar.peer] = nextIndex[ar.peer] - 1
				}

				// Shouldn't be necessary if log is persistent
				if nextIndex[ar.peer] <= matchIndex[ar.peer] {
					matchIndex[ar.peer] = nextIndex[ar.peer] - 1
				}

				// Ideally we'd only send for one client. Since we are resetting the heartbeat timer, we will send a heartbeat to all
				// Send in parallel so we don't wait for each client.
				for p, c := range peerClients {
					appendEntriesForClient(nextIndex[p], commandLog, currentTerm, id, commitIndex, c, p, appendResponseChan)
				}

				heartbeatTimer.Reset(HEARTBEAT_TIMEOUT)
			} else {
				// Success: client has appended
				if ar.messageIndex <= matchIndex[ar.peer] {
					continue
				}

				matchIndex[ar.peer] = ar.messageIndex
				nextIndex[ar.peer] = ar.messageIndex + 1

				commitIndex = getMajorityAgreementIndex(&matchIndex, int64(len(commandLog))-1+LOG_INDEXING)

				// TODO: If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
				for commitIndex > lastApplied && commandLog[commitIndex-LOG_INDEXING].Term == currentTerm {
					lastApplied = lastApplied + 1
					log.Printf("Applied command to store Op: %v Args: %v", commandLog[lastApplied-LOG_INDEXING].Cmd.Operation, commandLog[lastApplied-LOG_INDEXING].Cmd.Arg)
					go s.HandleCommand(responseLog[lastApplied-LOG_INDEXING])
				}
			}
		case <-heartbeatTimer.C:
			// The timer went off.
			// log.Printf("Heartbeat Timeout")

			if currentRole != LEADER {
				continue
			}

			for p, c := range peerClients {
				appendEntriesForClient(nextIndex[p], commandLog, currentTerm, id, commitIndex, c, p, appendResponseChan)
			}

			heartbeatTimer.Reset(HEARTBEAT_TIMEOUT)
		case <-dummyResponseChannel:
			// Ignore as this is only going to be populated on followers
		}
	}
	log.Printf("Strange to arrive here")
}
