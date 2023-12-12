// Copyright 2020, 2021, 2022, 2023 Red Hat, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consumer

import (
	"encoding/json"
	"errors"
	"github.com/RedHatInsights/insights-results-aggregator/metrics"
	"github.com/RedHatInsights/insights-results-aggregator/types"
	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"
	"time"
)

// OCPRulesProcessor satisfies MessageProcessor interface
type OCPRulesProcessor struct {
}

// ProcessMessage processes an incoming message
func (OCPRulesProcessor) ProcessMessage(consumer *KafkaConsumer, msg *sarama.ConsumerMessage) (types.RequestID, incomingMessage, error) {
	tStart := time.Now()

	log.Info().Int(offsetKey, int(msg.Offset)).Str(topicKey, consumer.Configuration.Topic).Str(groupKey, consumer.Configuration.Group).Msg("Consumed")

	message, err := consumer.parseMessage(msg, tStart)
	if err != nil {
		if errors.Is(err, types.ErrEmptyReport) {
			logMessageInfo(consumer, msg, &message, "This message has an empty report and will not be processed further")
			metrics.SkippedEmptyReports.Inc()
			return message.RequestID, message, nil
		}
		return message.RequestID, message, err
	}
	logMessageInfo(consumer, msg, &message, "Read")
	tRead := time.Now()

	checkMessageVersion(consumer, &message, msg)

	if ok, cause := checkMessageOrgInAllowList(consumer, &message, msg); !ok {
		err := errors.New(cause)
		logMessageError(consumer, msg, &message, cause, err)
		return message.RequestID, message, err
	}

	tAllowlisted := time.Now()

	logMessageDebug(consumer, msg, &message, "Marshalled")
	tMarshalled := time.Now()

	lastCheckedTime, err := consumer.retrieveLastCheckedTime(msg, &message)
	if err != nil {
		return message.RequestID, message, err
	}
	tTimeCheck := time.Now()

	// timestamp when the report is about to be written into database
	storedAtTime := time.Now()

	reportAsBytes, err := json.Marshal(*message.Report)
	if err != nil {
		logMessageError(consumer, msg, &message, "Error marshalling report", err)
		return message.RequestID, message, err
	}

	err = consumer.Storage.WriteReportForCluster(
		*message.Organization,
		*message.ClusterName,
		types.ClusterReport(reportAsBytes),
		message.ParsedHits,
		lastCheckedTime,
		message.Metadata.GatheredAt,
		storedAtTime,
		message.RequestID,
	)
	if err == types.ErrOldReport {
		logMessageInfo(consumer, msg, &message, "Skipping because a more recent report already exists for this cluster")
		return message.RequestID, message, nil
	} else if err != nil {
		logMessageError(consumer, msg, &message, "Error writing report to database", err)
		return message.RequestID, message, err
	}
	logMessageDebug(consumer, msg, &message, "Stored report")
	tStored := time.Now()

	tRecommendationsStored, err := consumer.writeRecommendations(msg, message, reportAsBytes)
	if err != nil {
		return message.RequestID, message, err
	}

	// rule hits has been stored into database - time to log all these great info
	logClusterInfo(&message)

	infoStoredAtTime := time.Now()
	if err := consumer.writeInfoReport(msg, message, infoStoredAtTime); err != nil {
		return message.RequestID, message, err
	}
	infoStored := time.Now()

	// log durations for every message consumption steps
	logDuration(tStart, tRead, msg.Offset, "read")
	logDuration(tRead, tAllowlisted, msg.Offset, "org_filtering")
	logDuration(tAllowlisted, tMarshalled, msg.Offset, "marshalling")
	logDuration(tMarshalled, tTimeCheck, msg.Offset, "time_check")
	logDuration(tTimeCheck, tStored, msg.Offset, "db_store_report")
	logDuration(tStored, tRecommendationsStored, msg.Offset, "db_store_recommendations")
	logDuration(infoStoredAtTime, infoStored, msg.Offset, "db_store_info_report")

	// message has been parsed and stored into storage
	return message.RequestID, message, nil
}
