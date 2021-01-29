package types_test

import (
	"testing"

	tmproto "github.com/tendermint/tendermint/proto/tendermint/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/authz/types"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	"github.com/stretchr/testify/require"
)

var (
	coin100 = sdk.NewInt64Coin("steak", 100)
	coin50  = sdk.NewInt64Coin("steak", 50)
	delAddr = sdk.AccAddress("_____delegator _____")
	val1    = sdk.ValAddress("_____validator1_____")
	val2    = sdk.ValAddress("_____validator2_____")
	val3    = sdk.ValAddress("_____validator3_____")
)

func TestDelegateAuthorizations(t *testing.T) {

	// verify MethodName
	delAuth := types.NewDelegateAuthorization([]sdk.ValAddress{val1, val2}, &coin100)
	require.Equal(t, delAuth.MethodName(), "/cosmos.staking.v1beta1.Msg/Delegate")

	testCases := []struct {
		msg                  string
		validators           []sdk.ValAddress
		limit                *sdk.Coin
		srvMsg               sdk.ServiceMsg
		expectErr            bool
		isDelete             bool
		updatedAuthorization *types.DelegateAuthorization
	}{
		{
			"expect 0 remaining coins",
			[]sdk.ValAddress{val1, val2},
			&coin100,
			createSrvMsgDelegate(delAuth.MethodName(), delAddr, val1, coin100),
			false,
			true,
			nil,
		},
		{
			"verify remaining coins",
			[]sdk.ValAddress{val1, val2},
			&coin100,
			createSrvMsgDelegate(delAuth.MethodName(), delAddr, val1, coin50),
			false,
			false,
			&types.DelegateAuthorization{ValidatorAddress: []string{val1.String(), val2.String()}, MaxTokens: &coin50},
		},
		{
			"testing with invalid validator",
			[]sdk.ValAddress{val1, val2},
			&coin100,
			createSrvMsgDelegate(delAuth.MethodName(), delAddr, val3, coin100),
			true,
			false,
			nil,
		},
		{
			"testing delegate without spent limit",
			[]sdk.ValAddress{val1, val2},
			nil,
			createSrvMsgDelegate(delAuth.MethodName(), delAddr, val2, coin100),
			false,
			false,
			&types.DelegateAuthorization{ValidatorAddress: []string{val1.String(), val2.String()}, MaxTokens: nil},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.msg, func(t *testing.T) {
			delAuth = types.NewDelegateAuthorization(tc.validators, tc.limit)
			updated, del, err := delAuth.Accept(tc.srvMsg, tmproto.Header{})
			if tc.expectErr {
				require.Error(t, err)
				require.Equal(t, tc.isDelete, del)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.isDelete, del)
				if tc.updatedAuthorization != nil {
					require.Equal(t, tc.updatedAuthorization.String(), updated.String())
				}
			}
		})
	}
}

func createSrvMsgDelegate(methodName string, delAddr sdk.AccAddress, valAddr sdk.ValAddress, amount sdk.Coin) sdk.ServiceMsg {
	msg := stakingtypes.NewMsgDelegate(delAddr, valAddr, amount)
	return sdk.ServiceMsg{
		MethodName: methodName,
		Request:    msg,
	}
}