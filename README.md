package main

import (
    "context"
    "encoding/json"
    "fmt"
    "math/rand"
    "os"
    "sync"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
    "github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    "github.com/google/uuid"
    "github.com/joho/godotenv"
    "github.com/robfig/cron/v3"
    "github.com/sirupsen/logrus"
)

var (
    log                  = logrus.New()
    cfg                  aws.Config
    dynamoClient         *dynamodb.Client
    sqsClient            *sqs.Client
    orderIDCounters      = make(map[string]int64)
    mu                   sync.Mutex
    balanceDataFromQueue3 BalanceData
    balanceDataFromQueue4 BalanceDataNetworks
)

type RequestMessage struct {
    UniqueID string `json:"unique_id"`
}

type BalanceData struct {
    TotalWorkspaceBalance float64 `json:"total_workspace_balance"`
    TotalFixedInvestment  float64 `json:"total_fixed_investment"`
    TotalBalance          float64 `json:"total_balance"`
    UniqueID              string  `json:"unique_id"`
    Timestamp             string  `json:"timestamp"`
}

type BalanceDataNetworks struct {
    Celo     float64 `json:"Celo"`
    Moonbeam float64 `json:"Moonbeam"`
    Polygon  float64 `json:"Polygon"`
    XRP      float64 `json:"xrp"` 
}

type MessageBody struct {
    UniqueID      string              `json:"UniqueID"`
    NetworkSupply BalanceDataNetworks `json:"networkSupply"`
    Timestamp     string              `json:"timestamp"`
}

type ComparisonResult struct {
    UniqueID         string `dynamodbav:"unique_id"`
    ComparisonResult bool   `dynamodbav:"comparison_result"`
    Timestamp        string `dynamodbav:"timestamp"`
}

func init() {
    log.Out = os.Stdout
    log.SetFormatter(&logrus.TextFormatter{
        FullTimestamp: true,
        ForceColors:   true,
    })
    log.SetLevel(logrus.InfoLevel)

    err := godotenv.Load()
    if err != nil {
        log.WithError(err).Fatal("Erro ao carregar o arquivo .env")
    }

    cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(os.Getenv("AWS_REGION")))
    if err != nil {
        log.WithError(err).Fatal("Erro ao carregar a configuração da AWS")
    }

    dynamoClient = dynamodb.NewFromConfig(cfg)
    rand.Seed(time.Now().UnixNano())

    sqsClient = sqs.NewFromConfig(cfg)
}

func main() {
    log.Info("Iniciando serviço...")

    
    c := cron.New()
    c.AddFunc("54 13 * * *", func() {
        results, err := readTable(dynamoClient, "poc-brla-comparison-results")
        if err != nil {
            log.WithError(err).Error("Erro ao ler a tabela DynamoDB")
            return
        }
        log.WithField("results", results).Info("Consulta realizada e dados enviados para a fila da nft")
    })
    c.Start()

    SqsReservationRequestSending()
    go ListenToSQSQueue3()
    go ListenToSQSQueue4()
    
    err := sendDataToFront(sqsClient)
    if err != nil {
        log.WithError(err).Error("Erro ao enviar dados para o front")
    }
    select {}
}

func readTable(svc *dynamodb.Client, tableName string) ([]ComparisonResult, error) {
    
    today := time.Now().Format("2006-01-02")
    log.Infof("Data de hoje: %s", today)

    
    params := &dynamodb.ScanInput{
        TableName:        aws.String(tableName),
        FilterExpression: aws.String("begins_with(#ts, :today)"),
        ExpressionAttributeNames: map[string]string{
            "#ts": "timestamp",
        },
        ExpressionAttributeValues: map[string]types.AttributeValue{
            ":today": &types.AttributeValueMemberS{Value: today},
        },
    }

    
    resp, err := svc.Scan(context.TODO(), params)
    if err != nil {
        log.WithError(err).Error("Erro ao escanear a tabela DynamoDB")
        return nil, err
    }

    
    log.Infof("Itens retornados: %v", resp.Items)

    
    for _, item := range resp.Items {
        uniqueID := item["unique_id"].(*types.AttributeValueMemberS).Value
        timestamp := item["timestamp"].(*types.AttributeValueMemberS).Value
        comparisonResult := item["comparison_result"].(*types.AttributeValueMemberBOOL).Value
        log.Infof("Atributos antes da deserialização: UniqueID=%s, Timestamp=%s, ComparisonResult=%v", uniqueID, timestamp, comparisonResult)
    }

    
    var results []ComparisonResult
    err = attributevalue.UnmarshalListOfMaps(resp.Items, &results)
    if err != nil {
        log.WithError(err).Error("Erro ao deserializar os resultados")
        return nil, err
    }

    
    for _, result := range results {
        log.Infof("Resultado deserializado: UniqueID=%s, Timestamp=%s, ComparisonResult=%v", result.UniqueID, result.Timestamp, result.ComparisonResult)
    }

    
    sqsClient := sqs.NewFromConfig(cfg)
    queueURL := os.Getenv("SQS_QUEUE_URL_6")

    messageBody, err := json.Marshal(results)
    if err != nil {
        log.WithError(err).Error("Erro ao serializar os resultados")
        return nil, err
    }

    _, err = sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
        QueueUrl:              aws.String(queueURL),
        MessageBody:           aws.String(string(messageBody)),
        MessageGroupId:        aws.String("default"), // Necessário para filas FIFO
        MessageDeduplicationId: aws.String(uuid.New().String()), // Identificador único para deduplicação
    })
    if err != nil {
        log.WithError(err).Error("Erro ao enviar mensagem para a fila SQS")
        return nil, err
    }

    return results, nil
}



func SqsReservationRequestSending() {
    endpoint1 := os.Getenv("SQS_ENDPOINT_1")
    if endpoint1 == "" {
        log.Fatal("SQS_ENDPOINT_1 não está definido")
    }

    endpoint2 := os.Getenv("SQS_ENDPOINT_2")
    if endpoint2 == "" {
        log.Fatal("SQS_ENDPOINT_2 não está definido")
    }

    svc1 := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
        o.EndpointResolver = sqs.EndpointResolverFromURL(endpoint1)
    })

    svc2 := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
        o.EndpointResolver = sqs.EndpointResolverFromURL(endpoint2)
    })

    message := RequestMessage{
        UniqueID: uuid.New().String(),
    }

    messageBody, err := json.Marshal(message)
    if err != nil {
        log.WithError(err).Fatal("Erro ao codificar a mensagem JSON")
    }

    queueURL1 := os.Getenv("SQS_QUEUE_URL_1")
    if queueURL1 == "" {
        log.Fatal("SQS_QUEUE_URL_1 não está definido")
    }

    queueURL2 := os.Getenv("SQS_QUEUE_URL_2")
    if queueURL2 == "" {
        log.Fatal("SQS_QUEUE_URL_2 não está definido")
    }

    
    _, err = svc1.SendMessage(context.TODO(), &sqs.SendMessageInput{
        MessageBody:            aws.String(string(messageBody)),
        QueueUrl:               aws.String(queueURL1),
        MessageGroupId:         aws.String("default"),
        MessageDeduplicationId: aws.String(uuid.New().String()),
    })
    if err != nil {
        log.WithError(err).Fatal("Erro ao enviar a mensagem para a fila SQS 1")
    }

    log.WithFields(logrus.Fields{
        "queue_url": queueURL1,
        "message":   string(messageBody),
    }).Info("\n\n====================\nMensagem enviada para a fila SQS 1 com sucesso\n====================\n")

    
    _, err = svc2.SendMessage(context.TODO(), &sqs.SendMessageInput{
        MessageBody:            aws.String(string(messageBody)),
        QueueUrl:               aws.String(queueURL2),
        MessageGroupId:         aws.String("default"),
        MessageDeduplicationId: aws.String(uuid.New().String()),
    })
    if err != nil {
        log.WithError(err).Fatal("Erro ao enviar a mensagem para a fila SQS 2")
    }

    log.WithFields(logrus.Fields{
        "queue_url": queueURL2,
        "message":   string(messageBody),
    }).Info("\n\n====================\nMensagem enviada para a fila SQS 2 com sucesso\n====================\n")
}


func InsertIntoDynamoDB(item map[string]types.AttributeValue, tableName string) error {
    input := &dynamodb.PutItemInput{
        TableName: aws.String(tableName),
        Item:      item,
    }

    _, err := dynamoClient.PutItem(context.TODO(), input)
    if err != nil {
        log.WithError(err).Error("Erro ao inserir dados na tabela DynamoDB")
        return err
    }

    log.WithFields(logrus.Fields{
        "order_id":  item["order_id"].(*types.AttributeValueMemberN).Value,
        "unique_id": item["unique_id"].(*types.AttributeValueMemberS).Value,
        "item":      item,
    }).Info("Dados inseridos na tabela DynamoDB com sucesso")
    return nil
}

func processAndCompareBalances(balanceData BalanceData, balanceDataNetworks BalanceDataNetworks) bool {
    
    totalConsultadoFiat := balanceData.TotalFixedInvestment + balanceData.TotalWorkspaceBalance

    
    totalConsultadoTokens := balanceDataNetworks.Celo + balanceDataNetworks.Moonbeam + balanceDataNetworks.Polygon + balanceDataNetworks.XRP

    
    totalConsultadoFiat = totalConsultadoFiat / 100

    timestamp := time.Now().Format(time.RFC3339)

    
    log.WithFields(logrus.Fields{
        "totalConsultadoFiat":   totalConsultadoFiat,
        "totalConsultadoTokens": totalConsultadoTokens,
        "unique_id":             balanceData.UniqueID,
        "timestamp":             timestamp,
    }).Info("Valores calculados para comparação")

    
    comparisonResult := totalConsultadoFiat >= totalConsultadoTokens

    
    err := InsertComparisonResultIntoDynamoDB(balanceData.UniqueID, timestamp, comparisonResult)
    if err != nil {
        log.WithError(err).Error("Erro ao inserir resultado da comparação na tabela DynamoDB")
    }

    return comparisonResult
}

func InsertComparisonResultIntoDynamoDB( uniqueID string, timestamp string, comparisonResult bool) error {
    item := map[string]types.AttributeValue{
        "unique_id":            &types.AttributeValueMemberS{Value: uniqueID},
        "timestamp":            &types.AttributeValueMemberS{Value: timestamp},
        "comparison_result":    &types.AttributeValueMemberBOOL{Value: comparisonResult},
    }

    input := &dynamodb.PutItemInput{
        TableName: aws.String("poc-brla-comparison-results"),
        Item:      item,
    }

    _, err := dynamoClient.PutItem(context.TODO(), input)
    if err != nil {
        log.WithError(err).Error("Erro ao inserir resultado da comparação na tabela DynamoDB")
        return err
    }

    log.WithFields(logrus.Fields{
        "unique_id":            uniqueID,
        "timestamp":            timestamp,
        "comparison_result":    comparisonResult,
    }).Info("Resultado da comparação inserido na tabela DynamoDB com sucesso")
    return nil
}


func ListenToSQSQueue3() {
    endpoint := os.Getenv("SQS_ENDPOINT_3")
    if endpoint == "" {
        log.Fatal("SQS_ENDPOINT_3 não está definido")
    }

    svc := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
        o.EndpointResolver = sqs.EndpointResolverFromURL(endpoint)
    })

    queueURL := os.Getenv("SQS_QUEUE_URL_3")
    if queueURL == "" {
        log.Fatal("SQS_QUEUE_URL_3 não está definido")
    }

    for {
        output, err := svc.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
            QueueUrl:            aws.String(queueURL),
            MaxNumberOfMessages: 1,
            WaitTimeSeconds:     10,
        })
        if err != nil {
            log.WithError(err).Error("Erro ao receber mensagem da fila SQS_retorno")
            continue
        }

        if len(output.Messages) > 0 {
            for _, message := range output.Messages {
                log.WithField("message", *message.Body).Info("Mensagem recebida da fila SQS_retorno")

                var balanceData BalanceData

                // Tentar deserializar como BalanceData
                err := json.Unmarshal([]byte(*message.Body), &balanceData)
                if err == nil && balanceData.UniqueID != "" {
                    log.WithFields(logrus.Fields{
                        "total_workspace_balance": balanceData.TotalWorkspaceBalance,
                        "total_fixed_investment":  balanceData.TotalFixedInvestment,
                        "total_balance":           balanceData.TotalBalance,
                        "unique_ID":               balanceData.UniqueID,
                        "timestamp":               balanceData.Timestamp,
                    }).Info("Dados de saldo recebidos")

                    orderID := getOrderID(balanceData.UniqueID)

                    item := map[string]types.AttributeValue{
                        "order_id":               &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", orderID)},
                        "unique_id":              &types.AttributeValueMemberS{Value: balanceData.UniqueID},
                        "total_workspace_balance": &types.AttributeValueMemberN{Value: fmt.Sprintf("%f", balanceData.TotalWorkspaceBalance)},
                        "total_fixed_investment":  &types.AttributeValueMemberN{Value: fmt.Sprintf("%f", balanceData.TotalFixedInvestment)},
                        "total_balance":           &types.AttributeValueMemberN{Value: fmt.Sprintf("%f", balanceData.TotalBalance)},
                        "timestamp":               &types.AttributeValueMemberS{Value: balanceData.Timestamp}, // Adicionando o timestamp
                    }

                    
                    log.WithFields(logrus.Fields{
                        "order_id":               orderID,
                        "unique_id":              balanceData.UniqueID,
                        "total_workspace_balance": balanceData.TotalWorkspaceBalance,
                        "total_fixed_investment":  balanceData.TotalFixedInvestment,
                        "total_balance":           balanceData.TotalBalance,
                        "timestamp":               balanceData.Timestamp, 
                    }).Info("Dados prontos para inserção no DynamoDB")

                    err = InsertIntoDynamoDB(item, "poc-brla-supply")
                    if err != nil {
                        log.WithError(err).Error("Erro ao inserir dados na tabela DynamoDB")
                        continue
                    }

                    
                    mu.Lock()
                    balanceDataFromQueue3 = balanceData
                    mu.Unlock()

                    
                    mu.Lock()
                    if balanceDataFromQueue3.UniqueID != "" {
                        result := processAndCompareBalances(balanceDataFromQueue3, balanceDataFromQueue4)
                        log.WithField("comparison_result", result).Info("Resultado da comparação de saldos")
                    }
                    mu.Unlock()
                } else {
                    log.WithError(err).Error("Erro ao deserializar a mensagem JSON")
                    continue
                }

               
                _, err = svc.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
                    QueueUrl:      aws.String(queueURL),
                    ReceiptHandle: message.ReceiptHandle,
                })
                if err != nil {
                    log.WithError(err).Error("Erro ao deletar mensagem da fila SQS")
                }
            }
        }
    }
}

func ListenToSQSQueue4() {
    endpoint := os.Getenv("SQS_ENDPOINT_4")
    if endpoint == "" {
        log.Fatal("SQS_ENDPOINT_4 não está definido")
    }

    svc := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
        o.EndpointResolver = sqs.EndpointResolverFromURL(endpoint)
    })

    queueURL := os.Getenv("SQS_QUEUE_URL_4")
    if queueURL == "" {
        log.Fatal("SQS_QUEUE_URL_4 não está definido")
    }

    for {
        output, err := svc.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
            QueueUrl:            aws.String(queueURL),
            MaxNumberOfMessages: 1,
            WaitTimeSeconds:     10,
        })
        if err != nil {
            log.WithError(err).Error("Erro ao receber mensagem da fila SQS_retorno")
            continue
        }

        if len(output.Messages) > 0 {
            for _, message := range output.Messages {
                log.WithField("message", *message.Body).Info("Mensagem recebida da fila SQS_retorno")

                var messageBody MessageBody

                
                log.WithField("raw_message", *message.Body).Info("Mensagem JSON recebida")

                
                err := json.Unmarshal([]byte(*message.Body), &messageBody)
                if err != nil {
                    log.WithError(err).Error("Erro ao deserializar a mensagem JSON")
                    continue
                }

                if messageBody.UniqueID != "" {
                    log.WithFields(logrus.Fields{
                        "Celo":      messageBody.NetworkSupply.Celo,
                        "Moonbeam":  messageBody.NetworkSupply.Moonbeam,
                        "Polygon":   messageBody.NetworkSupply.Polygon,
                        "XRP":       messageBody.NetworkSupply.XRP,
                        "unique_ID": messageBody.UniqueID,
                        "timestamp": messageBody.Timestamp,
                    }).Info("Dados de saldo de redes recebidos")

                    orderID := getOrderID(messageBody.UniqueID)

                    item := map[string]types.AttributeValue{
                        "order_id":  &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", orderID)},
                        "unique_id": &types.AttributeValueMemberS{Value: messageBody.UniqueID},
                        "Celo":      &types.AttributeValueMemberN{Value: fmt.Sprintf("%f", messageBody.NetworkSupply.Celo)},
                        "Moonbeam":  &types.AttributeValueMemberN{Value: fmt.Sprintf("%f", messageBody.NetworkSupply.Moonbeam)},
                        "Polygon":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%f", messageBody.NetworkSupply.Polygon)},
                        "XRP":       &types.AttributeValueMemberN{Value: fmt.Sprintf("%f", messageBody.NetworkSupply.XRP)},
                        "timestamp": &types.AttributeValueMemberS{Value: messageBody.Timestamp}, 
                    }

                    
                    log.WithFields(logrus.Fields{
                        "order_id":  orderID,
                        "unique_id": messageBody.UniqueID,
                        "Celo":      messageBody.NetworkSupply.Celo,
                        "Moonbeam":  messageBody.NetworkSupply.Moonbeam,
                        "Polygon":   messageBody.NetworkSupply.Polygon,
                        "XRP":       messageBody.NetworkSupply.XRP,
                        "timestamp": messageBody.Timestamp, 
                    }).Info("Dados prontos para inserção no DynamoDB")

                    err = InsertIntoDynamoDB(item, "poc-brla-supply")
                    if err != nil {
                        log.WithError(err).Error("Erro ao inserir dados na tabela DynamoDB")
                        continue
                    }

                    
                    mu.Lock()
                    balanceDataFromQueue4 = messageBody.NetworkSupply
                    mu.Unlock()

                    
                    mu.Lock()
                    if balanceDataFromQueue3.UniqueID != "" {
                        result := processAndCompareBalances(balanceDataFromQueue3, balanceDataFromQueue4)
                        log.WithField("comparison_result", result).Info("Resultado da comparação de saldos")
                    }
                    mu.Unlock()
                } else {
                    log.WithError(err).Error("Erro ao deserializar a mensagem JSON")
                    continue
                }

                
                _, err = svc.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
                    QueueUrl:      aws.String(queueURL),
                    ReceiptHandle: message.ReceiptHandle,
                })
                if err != nil {
                    log.WithError(err).Error("Erro ao deletar mensagem da fila SQS")
                }
            }
        }
    }
}

func sendDataToFront(sqsClient *sqs.Client) error {
    
    currentMonth := time.Now().Format("2006-01")
    logrus.Infof("Mês atual: %s", currentMonth)

    
    scanTable := func(tableName string) ([]map[string]types.AttributeValue, error) {
        params := &dynamodb.ScanInput{
            TableName:        aws.String(tableName),
            FilterExpression: aws.String("begins_with(#ts, :currentMonth)"),
            ExpressionAttributeNames: map[string]string{
                "#ts": "timestamp",
            },
            ExpressionAttributeValues: map[string]types.AttributeValue{
                ":currentMonth": &types.AttributeValueMemberS{Value: currentMonth},
            },
        }

        
        resp, err := dynamoClient.Scan(context.TODO(), params)
        if err != nil {
            logrus.WithError(err).Errorf("Erro ao escanear a tabela %s", tableName)
            return nil, err
        }

        return resp.Items, nil
    }

    
    comparisonResults, err := scanTable("poc-brla-comparison-results")
    if err != nil {
        return err
    }

    
    supplyResults, err := scanTable("poc-brla-supply")
    if err != nil {
        return err
    }

    
    logrus.Infof("Resultados da tabela poc-brla-comparison-results: %v", comparisonResults)
    logrus.Infof("Resultados da tabela poc-brla-supply: %v", supplyResults)

    
    data := map[string]interface{}{
        "comparisonResults": comparisonResults,
        "supplyResults":     supplyResults,
    }
    jsonData, err := json.Marshal(data)
    if err != nil {
        logrus.WithError(err).Error("Erro ao serializar os dados em JSON")
        return err
    }


    
    queueURL := os.Getenv("SQS_QUEUE_URL_7")
    if queueURL == "" {
        logrus.Error("SQS_QUEUE_URL_7 não está definido")
        return fmt.Errorf("SQS_QUEUE_URL_7 não está definido")
    }

    
    _, err = sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
        QueueUrl:              aws.String(queueURL),
        MessageBody:           aws.String(string(jsonData)),
        MessageGroupId:        aws.String("default"), 
        MessageDeduplicationId: aws.String(uuid.New().String()), 
    })
    if err != nil {
        logrus.WithError(err).Error("Erro ao enviar mensagem para a fila SQS")
        return err
    }

    logrus.Info("Dados enviados para a fila SQS com sucesso")
    return nil
}


func getOrderID(uniqueID string) int64 {
    mu.Lock()
    defer mu.Unlock()

    if _, exists := orderIDCounters[uniqueID]; !exists {
        orderIDCounters[uniqueID] = 1
    } else {
        orderIDCounters[uniqueID]++
    }

    return orderIDCounters[uniqueID]
}
