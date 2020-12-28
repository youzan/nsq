package nsqadmin

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"

	"github.com/gorilla/sessions"
)

var store = sessions.NewCookieStore([]byte("user-authentication"))

func init() {
	gob.Register(&CasUserModel{})
}

type CasResponse struct {
	Code int
	Msg  string
	Data *CasResponseValue
}

type CasResponseValue struct {
	Value *CasUserModel
}

type CasUserModel struct {
	Id       int64
	UserName string
	Gender   bool
	RealName string
	Aliasna  string
	Mobile   string
	Email    string
	Login    bool
	ctx      *Context
}

func (u *CasUserModel) String() string {
	return fmt.Sprintf("Id: %v, Username: %v, Realname: %v, Email: %v, Login: %v", u.Id, u.UserName, u.RealName, u.Email, u.Login)
}

func (u *CasUserModel) copy(newU *CasUserModel) {
	u.Id = newU.Id
	u.UserName = newU.UserName
	u.Gender = newU.Gender
	u.RealName = newU.RealName
	u.Aliasna = newU.Aliasna
	u.Mobile = newU.Mobile
	u.Email = newU.Email
}

func (u *CasUserModel) getId() int64 {
	return u.Id
}

func (u *CasUserModel) GetUserName() string {
	return u.UserName
}

func (u *CasUserModel) getGender() bool {
	return u.Gender
}

func (u *CasUserModel) getRealName() string {
	return u.RealName
}

func (u *CasUserModel) getAliasna() string {
	return u.Aliasna
}

func (u *CasUserModel) getMobile() string {
	return u.Mobile
}

func (u *CasUserModel) getEmail() string {
	return u.Email
}

func NewCasUserModel(ctx *Context, w http.ResponseWriter, req *http.Request) (*CasUserModel, error) {
	casUser := &CasUserModel{
		Login: false,
		ctx:   ctx,
	}
	session, err := store.Get(req, "session-user")
	if err != nil {
		ctx.nsqadmin.logf("ERROR: error in fetching session, err: %s", err)
		return nil, err
	}
	session.Options = &sessions.Options{
		Path: "/",
		//keep it in 12 hours
		MaxAge: 43200,
	}
	session.Values["userModel"] = casUser
	session.Save(req, w)
	return casUser, nil
}

func (u *CasUserModel) SetContext(ctx *Context) error {
	u.ctx = ctx
	return nil
}

func (u *CasUserModel) IsLogin() bool {
	return u.Login
}

func (u *CasUserModel) GetUserRole() string {
	return "user"
}

func (u *CasUserModel) IsAdmin() bool {
	if login := u.IsLogin(); !login {
		return false
	}
	if ac := u.ctx.nsqadmin.ac; ac == nil {
		return true
	} else {
		return ac.IsAdmin(u.UserName)
	}
}

//save(persist) current case user model
func (u *CasUserModel) save(w http.ResponseWriter, req *http.Request) error {
	session, err := store.Get(req, "session-user")
	if err != nil {
		u.ctx.nsqadmin.logf("ERROR: error in fetching session, err %v", err)
		return err
	}
	session.Save(req, w)
	return nil
}

func (u *CasUserModel) DoAuth(w http.ResponseWriter, req *http.Request) error {
	v, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		u.ctx.nsqadmin.logf("ERROR: fail to parse cas URL queries %v", req.URL.String())
		return err
	}
	code := v.Get("code")
	if code == "" {
		return errors.New("illeggal code form cas")
	}

	casUrl := u.ctx.nsqadmin.opts.AuthUrl
	userInfoUrl, err := url.Parse(casUrl)
	if err != nil {
		u.ctx.nsqadmin.logf("ERROR: fail to parse cas URL %v", casUrl)
		return err
	}
	userInfoUrl.Path = "/oauth/users/self"
	v, err = url.ParseQuery(userInfoUrl.RawQuery)
	if err != nil {
		u.ctx.nsqadmin.logf("ERROR: fail to parse cas URL queries %v", casUrl)
		return err
	}
	v.Add("code", code)
	userInfoUrl.RawQuery = v.Encode()

	casSecret := u.ctx.nsqadmin.opts.AuthSecret
	client := http.DefaultClient
	userInfoReq, _ := http.NewRequest("GET", userInfoUrl.String(), nil)
	userInfoReq.Header.Set("Authorization", "oauth "+casSecret)
	resp, err := client.Do(userInfoReq)
	if err != nil {
		u.ctx.nsqadmin.logf("WARN: fail to fetch user info from cas remote, url %v, err: %v", userInfoUrl.String(), err)
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	authResp := &CasResponse{}
	err = json.Unmarshal(body, authResp)
	if err != nil {
		u.ctx.nsqadmin.logf("WARN: fail to parse user info from cas remote, url %v, err: %v", userInfoUrl.String(), err)
		//redirect
		return err
	}
	if authResp.Code != 0 {
		u.ctx.nsqadmin.logf("WARN: wrong resp code in user info resp, url %v, code: %v", userInfoUrl.String(), authResp.Code)
		return errors.New(fmt.Sprintf("wrong resp code %v in user info resp", authResp.Code))
	}
	u.copy(authResp.Data.Value)
	u.Login = true
	u.save(w, req)
	return nil
}

func GetUserModel(ctx *Context, req *http.Request) (IUserAuth, error) {
	session, err := store.Get(req, "session-user")
	if err != nil {
		return nil, err
	}
	val := session.Values["userModel"]
	if val != nil {
		if userModel, ok := val.(IUserAuth); !ok {
			return nil, fmt.Errorf("cas value type in session is not right, actual type %v", reflect.TypeOf(val))
		} else {
			if userModel != nil {
				userModel.SetContext(ctx)
			}
			return userModel, nil
		}
	} else {
		return nil, nil
	}
}

func LogoutUser(ctx *Context, w http.ResponseWriter, req *http.Request) error {
	session, err := store.Get(req, "session-user")
	if err != nil {
		return err
	}
	val := session.Values["userModel"]
	if casUserModel, ok := val.(IUserAuth); ok {
		ctx.nsqadmin.logf("ACCESS: user: %v logout", casUserModel)
	} else {
		ctx.nsqadmin.logf("WARNING: user info not found")
	}
	session.Values["userModel"] = nil
	return session.Save(req, w)
}

type IUserAuth interface {
	IsLogin() bool
	GetUserRole() string
	GetUserName() string
	IsAdmin() bool
	DoAuth(w http.ResponseWriter, req *http.Request) error
	SetContext(ctx *Context) error
	String() string
}
