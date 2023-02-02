package models

import (
	"time"
	"github.com/pkg/errors"

	"github.com/gofrs/uuid"
)

type OAuthState struct {
	ID                  uuid.UUID `json:"id" db:"id"`
	InternalAuthCode    string    `json:"internal_auth_code" db:"internal_auth_code"`
	HashedCodeChallenge string    `json:"hashed_code_challenge" db:"hashed_code_challenge"`
	ProviderType        string    `json:"provider_type" db:"provider_type"`
	CreatedAt           time.Time `json:"created_at" db:"created_at"`
	UpdatedAt           time.Time `json:"updated_at" db:"updated_at"`
}


func (OAuthState) TableName() string {
	tableName := "oauth_state"
	return tableName
}


func NewOAuthState(internalAuthCode, providerType, hashedChallenge string) (*OAuthState, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, errors.Wrap("error generating unique oauth state verifier")
	}
	oauth := &OAuthState{
		ID: id,
		ProviderType: providerType,
		HashedCodeChallenge: hashedChallenge,
		InternalAuthCode: internalAuthCode,
	}
	return oauth, nil
}
