package main

import (
  "context"
  "fmt"
  "github.com/alecthomas/kong"
  tea "github.com/charmbracelet/bubbletea"
  "github.com/charmbracelet/lipgloss"
  "github.com/docker/docker/api/types/container"
  "github.com/docker/go-connections/nat"
  "github.com/dustin/go-humanize"
  "github.com/hamdan/kgo-cooperative-bug-reproducer/tui"
  "github.com/pkg/errors"
  "github.com/testcontainers/testcontainers-go"
  "github.com/testcontainers/testcontainers-go/modules/kafka"
  "github.com/twmb/franz-go/pkg/kadm"
  "github.com/twmb/franz-go/pkg/kgo"
  "log"
  "slices"
  "strings"
  "sync"
  "time"
)

const (
  topic         = "test-topic"
  consumerGroup = "test-group"
)

type portFlag struct {
  Port string `short:"p" required:"" help:"The exposed port for the Kafka cluster."`
}

var cli struct {
  Kafka struct{} `cmd:"" help:"Run the Kafka cluster."`

  Producer struct {
    portFlag
    Delay time.Duration `short:"d" default:"10ms" help:"The delay in milliseconds between producing messages."`
  } `cmd:"" help:"Run the producer."`

  CooperativeSticky portFlag `cmd:"" help:"Run the cooperative sticky consumer."`
  Range             portFlag `cmd:"" help:"Run the range consumer."`
  Both              portFlag `cmd:"" help:"Run the cooperative sticky+range consumer."`
}

func main() {
  kctx := kong.Parse(&cli)

  switch kctx.Command() {
  case "kafka":
    runKafka()
  case "both":
    runConsumer("both", cli.Both.Port, kgo.CooperativeStickyBalancer(), kgo.RangeBalancer())
  case "range":
    runConsumer("range", cli.Range.Port, kgo.RangeBalancer())
  case "cooperative-sticky":
    runConsumer("cooperative-sticky", cli.CooperativeSticky.Port, kgo.CooperativeStickyBalancer())
  case "producer":
    runProducer()
  default:
    panic(kctx.Command())
  }
}

func runKafka() {
  ctx := context.Background()

  kafkaContainer, err := kafka.RunContainer(
    ctx,
    kafka.WithClusterID("kafka-cluster"),
    testcontainers.WithImage("confluentinc/confluent-local:7.6.0"),
    testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
      if hostConfig.PortBindings == nil {
        hostConfig.PortBindings = make(map[nat.Port][]nat.PortBinding)
      }
      hostConfig.PortBindings["9092/tcp"] = []nat.PortBinding{{HostIP: "localhost", HostPort: "9092"}}
    }),
  )
  if err != nil {
    log.Fatalf("failed to start container: %s", err)
  }

  brokers, err := kafkaContainer.Brokers(ctx)

  p := tea.NewProgram(tui.NewSpinner(fmt.Sprintf("Running Kafka on %s", brokers[0]), nil))
  if _, err := p.Run(); err != nil {
    log.Fatalf("failed to spin: %s", err)
  }

  defer func() {
    if err := kafkaContainer.Terminate(ctx); err != nil {
      log.Fatalf("failed to terminate container: %s", err)
    }
  }()
}

func runConsumer(id string, port string, balancers ...kgo.GroupBalancer) {
  client, err := kgo.NewClient(
    kgo.SeedBrokers(fmt.Sprintf("localhost:%s", port)),
    kgo.ClientID(id),

    kgo.Balancers(balancers...),
    kgo.ConsumerGroup(consumerGroup),
    kgo.ConsumeTopics(topic),
  )
  if err != nil {
    log.Fatalf("failed to create client: %s", err)
  }
  defer client.Close()

  adm := kadm.NewClient(client)
  defer adm.Close()
  topics, err := adm.ListTopics(context.Background(), topic)
  if err != nil {
    log.Fatalf("failed to list topics: %s", err)

  }
  partitionDetails := topics[topic].Partitions
  partitions := make([]int32, 0, len(partitionDetails))
  for p := range partitionDetails {
    partitions = append(partitions, p)
  }
  slices.Sort(partitions)

  wg := sync.WaitGroup{}
  pollingCtx, pollingCtxCancel := context.WithCancel(context.Background())
  consumingMsgChan := make(chan string, 1)

  wg.Add(1)
  go func() {
    defer wg.Done()

    defer pollingCtxCancel()

    p := tea.NewProgram(tui.NewSpinner(fmt.Sprintf("Running %s consumer", id), &consumingMsgChan))
    if _, err := p.Run(); err != nil {
      log.Fatalf("failed to spin: %s", err)
    }
  }()

  wg.Add(1)
  go func() {
    defer wg.Done()

    fetchCountByPartition := map[int32]int{}

    for {
      select {
      case <-pollingCtx.Done():
        return
      default:
        fetches := client.PollFetches(pollingCtx)
        fetches.EachPartition(func(topicPartition kgo.FetchTopicPartition) {
          fetchCountByPartition[topicPartition.Partition] += len(topicPartition.Records)
        })

        msgs := []string{fmt.Sprintf("Running %s consumer", id), "", "Partition |     Count", "--------- | ---------"}
        for _, p := range partitions {
          count, ok := fetchCountByPartition[p]
          if ok {
            msgs = append(msgs, fmt.Sprintf("%9d | %9d", p, count))
          } else {
            msgs = append(msgs, fmt.Sprintf("%9d |         -", p))
          }
        }

        select {
        case consumingMsgChan <- lipgloss.JoinVertical(lipgloss.Left, msgs...):
        default:
        }
      }
    }
  }()

  go func() {
    time.Sleep(5 * time.Second)

    client.ForceRebalance()
  }()

  wg.Wait()
}

func runProducer() {
  client, err := kgo.NewClient(
    kgo.SeedBrokers(fmt.Sprintf("localhost:%s", cli.Producer.Port)),
    kgo.ClientID("producer"),

    kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.SnappyCompression()),
    kgo.RecordPartitioner(kgo.ManualPartitioner()),
    kgo.ProducerLinger(0),
  )
  if err != nil {
    log.Fatalf("failed to create client: %s", err)
  }
  defer client.Close()

  adm := kadm.NewClient(client)
  resp, err := adm.CreateTopic(context.Background(), 16, 1, map[string]*string{}, topic)
  if err != nil && !strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
    log.Fatalf("failed to create topic: %s %s", err, resp.Err)
  }

  topics, err := adm.ListTopics(context.Background(), topic)
  if err != nil {
    log.Fatalf("failed to list topics: %s", err)

  }
  partitions := topics[topic].Partitions

  wg := sync.WaitGroup{}
  producingCtx, producingCtxCancel := context.WithCancel(context.Background())
  producingMsgChan := make(chan string, 1)

  wg.Add(1)
  go func() {
    defer wg.Done()

    defer producingCtxCancel()

    p := tea.NewProgram(tui.NewSpinner("Running producer", &producingMsgChan))
    if _, err := p.Run(); err != nil {
      log.Fatalf("failed to spin: %s", err)
    }
  }()

  wg.Add(1)
  go func() {
    defer wg.Done()

    var count int64 = 0

    for {
      select {
      case <-producingCtx.Done():
        return
      default:
        for p := range partitions {
          p := p
          res := client.ProduceSync(producingCtx, &kgo.Record{
            Partition: p,
            Topic:     topic,
          })
          if res.FirstErr() != nil {
            if errors.Is(res.FirstErr(), context.Canceled) {
              return // exit cleanly
            }
            log.Fatalf("failed to produce: %s", res.FirstErr())
          }

          count++
          select {
          case producingMsgChan <- fmt.Sprintf("Running producer (%s msgs produced)", humanize.Comma(count)):
          default:
          }

          time.Sleep(cli.Producer.Delay)
        }
      }
    }
  }()

  wg.Wait()
}
