/*! For license information please see 8b143d3d.b68d30b6.js.LICENSE.txt */
"use strict";(self.webpackChunkbackstage_microsite=self.webpackChunkbackstage_microsite||[]).push([[530325],{732190:(e,r,t)=>{t.r(r),t.d(r,{assets:()=>s,contentTitle:()=>u,default:()=>f,frontMatter:()=>i,metadata:()=>c,toc:()=>a});var n=t(824246),o=t(511151);const i={id:"plugin-scaffolder-node.serializedfile.symlink",title:"SerializedFile.symlink",description:"API reference for SerializedFile.symlink"},u=void 0,c={id:"reference/plugin-scaffolder-node.serializedfile.symlink",title:"SerializedFile.symlink",description:"API reference for SerializedFile.symlink",source:"@site/../docs/reference/plugin-scaffolder-node.serializedfile.symlink.md",sourceDirName:"reference",slug:"/reference/plugin-scaffolder-node.serializedfile.symlink",permalink:"/docs/reference/plugin-scaffolder-node.serializedfile.symlink",draft:!1,unlisted:!1,editUrl:"https://github.com/backstage/backstage/edit/master/docs/../docs/reference/plugin-scaffolder-node.serializedfile.symlink.md",tags:[],version:"current",frontMatter:{id:"plugin-scaffolder-node.serializedfile.symlink",title:"SerializedFile.symlink",description:"API reference for SerializedFile.symlink"}},s={},a=[];function l(e){const r={a:"a",code:"code",p:"p",pre:"pre",strong:"strong",...(0,o.a)(),...e.components};return(0,n.jsxs)(n.Fragment,{children:[(0,n.jsxs)(r.p,{children:[(0,n.jsx)(r.a,{href:"/docs/reference/",children:"Home"})," > ",(0,n.jsx)(r.a,{href:"/docs/reference/plugin-scaffolder-node",children:(0,n.jsx)(r.code,{children:"@backstage/plugin-scaffolder-node"})})," > ",(0,n.jsx)(r.a,{href:"/docs/reference/plugin-scaffolder-node.serializedfile",children:(0,n.jsx)(r.code,{children:"SerializedFile"})})," > ",(0,n.jsx)(r.a,{href:"/docs/reference/plugin-scaffolder-node.serializedfile.symlink",children:(0,n.jsx)(r.code,{children:"symlink"})})]}),"\n",(0,n.jsx)(r.p,{children:(0,n.jsx)(r.strong,{children:"Signature:"})}),"\n",(0,n.jsx)(r.pre,{children:(0,n.jsx)(r.code,{className:"language-typescript",children:"symlink?: boolean;\n"})})]})}function f(e={}){const{wrapper:r}={...(0,o.a)(),...e.components};return r?(0,n.jsx)(r,{...e,children:(0,n.jsx)(l,{...e})}):l(e)}},371426:(e,r,t)=>{var n=t(827378),o=Symbol.for("react.element"),i=Symbol.for("react.fragment"),u=Object.prototype.hasOwnProperty,c=n.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED.ReactCurrentOwner,s={key:!0,ref:!0,__self:!0,__source:!0};function a(e,r,t){var n,i={},a=null,l=null;for(n in void 0!==t&&(a=""+t),void 0!==r.key&&(a=""+r.key),void 0!==r.ref&&(l=r.ref),r)u.call(r,n)&&!s.hasOwnProperty(n)&&(i[n]=r[n]);if(e&&e.defaultProps)for(n in r=e.defaultProps)void 0===i[n]&&(i[n]=r[n]);return{$$typeof:o,type:e,key:a,ref:l,props:i,_owner:c.current}}r.Fragment=i,r.jsx=a,r.jsxs=a},541535:(e,r)=>{var t=Symbol.for("react.element"),n=Symbol.for("react.portal"),o=Symbol.for("react.fragment"),i=Symbol.for("react.strict_mode"),u=Symbol.for("react.profiler"),c=Symbol.for("react.provider"),s=Symbol.for("react.context"),a=Symbol.for("react.forward_ref"),l=Symbol.for("react.suspense"),f=Symbol.for("react.memo"),d=Symbol.for("react.lazy"),p=Symbol.iterator;var y={isMounted:function(){return!1},enqueueForceUpdate:function(){},enqueueReplaceState:function(){},enqueueSetState:function(){}},m=Object.assign,h={};function _(e,r,t){this.props=e,this.context=r,this.refs=h,this.updater=t||y}function b(){}function v(e,r,t){this.props=e,this.context=r,this.refs=h,this.updater=t||y}_.prototype.isReactComponent={},_.prototype.setState=function(e,r){if("object"!=typeof e&&"function"!=typeof e&&null!=e)throw Error("setState(...): takes an object of state variables to update or a function which returns an object of state variables.");this.updater.enqueueSetState(this,e,r,"setState")},_.prototype.forceUpdate=function(e){this.updater.enqueueForceUpdate(this,e,"forceUpdate")},b.prototype=_.prototype;var k=v.prototype=new b;k.constructor=v,m(k,_.prototype),k.isPureReactComponent=!0;var S=Array.isArray,g=Object.prototype.hasOwnProperty,j={current:null},x={key:!0,ref:!0,__self:!0,__source:!0};function E(e,r,n){var o,i={},u=null,c=null;if(null!=r)for(o in void 0!==r.ref&&(c=r.ref),void 0!==r.key&&(u=""+r.key),r)g.call(r,o)&&!x.hasOwnProperty(o)&&(i[o]=r[o]);var s=arguments.length-2;if(1===s)i.children=n;else if(1<s){for(var a=Array(s),l=0;l<s;l++)a[l]=arguments[l+2];i.children=a}if(e&&e.defaultProps)for(o in s=e.defaultProps)void 0===i[o]&&(i[o]=s[o]);return{$$typeof:t,type:e,key:u,ref:c,props:i,_owner:j.current}}function w(e){return"object"==typeof e&&null!==e&&e.$$typeof===t}var R=/\/+/g;function C(e,r){return"object"==typeof e&&null!==e&&null!=e.key?function(e){var r={"=":"=0",":":"=2"};return"$"+e.replace(/[=:]/g,(function(e){return r[e]}))}(""+e.key):r.toString(36)}function $(e,r,o,i,u){var c=typeof e;"undefined"!==c&&"boolean"!==c||(e=null);var s=!1;if(null===e)s=!0;else switch(c){case"string":case"number":s=!0;break;case"object":switch(e.$$typeof){case t:case n:s=!0}}if(s)return u=u(s=e),e=""===i?"."+C(s,0):i,S(u)?(o="",null!=e&&(o=e.replace(R,"$&/")+"/"),$(u,r,o,"",(function(e){return e}))):null!=u&&(w(u)&&(u=function(e,r){return{$$typeof:t,type:e.type,key:r,ref:e.ref,props:e.props,_owner:e._owner}}(u,o+(!u.key||s&&s.key===u.key?"":(""+u.key).replace(R,"$&/")+"/")+e)),r.push(u)),1;if(s=0,i=""===i?".":i+":",S(e))for(var a=0;a<e.length;a++){var l=i+C(c=e[a],a);s+=$(c,r,o,l,u)}else if(l=function(e){return null===e||"object"!=typeof e?null:"function"==typeof(e=p&&e[p]||e["@@iterator"])?e:null}(e),"function"==typeof l)for(e=l.call(e),a=0;!(c=e.next()).done;)s+=$(c=c.value,r,o,l=i+C(c,a++),u);else if("object"===c)throw r=String(e),Error("Objects are not valid as a React child (found: "+("[object Object]"===r?"object with keys {"+Object.keys(e).join(", ")+"}":r)+"). If you meant to render a collection of children, use an array instead.");return s}function O(e,r,t){if(null==e)return e;var n=[],o=0;return $(e,n,"","",(function(e){return r.call(t,e,o++)})),n}function P(e){if(-1===e._status){var r=e._result;(r=r()).then((function(r){0!==e._status&&-1!==e._status||(e._status=1,e._result=r)}),(function(r){0!==e._status&&-1!==e._status||(e._status=2,e._result=r)})),-1===e._status&&(e._status=0,e._result=r)}if(1===e._status)return e._result.default;throw e._result}var z={current:null},I={transition:null},F={ReactCurrentDispatcher:z,ReactCurrentBatchConfig:I,ReactCurrentOwner:j};r.Children={map:O,forEach:function(e,r,t){O(e,(function(){r.apply(this,arguments)}),t)},count:function(e){var r=0;return O(e,(function(){r++})),r},toArray:function(e){return O(e,(function(e){return e}))||[]},only:function(e){if(!w(e))throw Error("React.Children.only expected to receive a single React element child.");return e}},r.Component=_,r.Fragment=o,r.Profiler=u,r.PureComponent=v,r.StrictMode=i,r.Suspense=l,r.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED=F,r.cloneElement=function(e,r,n){if(null==e)throw Error("React.cloneElement(...): The argument must be a React element, but you passed "+e+".");var o=m({},e.props),i=e.key,u=e.ref,c=e._owner;if(null!=r){if(void 0!==r.ref&&(u=r.ref,c=j.current),void 0!==r.key&&(i=""+r.key),e.type&&e.type.defaultProps)var s=e.type.defaultProps;for(a in r)g.call(r,a)&&!x.hasOwnProperty(a)&&(o[a]=void 0===r[a]&&void 0!==s?s[a]:r[a])}var a=arguments.length-2;if(1===a)o.children=n;else if(1<a){s=Array(a);for(var l=0;l<a;l++)s[l]=arguments[l+2];o.children=s}return{$$typeof:t,type:e.type,key:i,ref:u,props:o,_owner:c}},r.createContext=function(e){return(e={$$typeof:s,_currentValue:e,_currentValue2:e,_threadCount:0,Provider:null,Consumer:null,_defaultValue:null,_globalName:null}).Provider={$$typeof:c,_context:e},e.Consumer=e},r.createElement=E,r.createFactory=function(e){var r=E.bind(null,e);return r.type=e,r},r.createRef=function(){return{current:null}},r.forwardRef=function(e){return{$$typeof:a,render:e}},r.isValidElement=w,r.lazy=function(e){return{$$typeof:d,_payload:{_status:-1,_result:e},_init:P}},r.memo=function(e,r){return{$$typeof:f,type:e,compare:void 0===r?null:r}},r.startTransition=function(e){var r=I.transition;I.transition={};try{e()}finally{I.transition=r}},r.unstable_act=function(){throw Error("act(...) is not supported in production builds of React.")},r.useCallback=function(e,r){return z.current.useCallback(e,r)},r.useContext=function(e){return z.current.useContext(e)},r.useDebugValue=function(){},r.useDeferredValue=function(e){return z.current.useDeferredValue(e)},r.useEffect=function(e,r){return z.current.useEffect(e,r)},r.useId=function(){return z.current.useId()},r.useImperativeHandle=function(e,r,t){return z.current.useImperativeHandle(e,r,t)},r.useInsertionEffect=function(e,r){return z.current.useInsertionEffect(e,r)},r.useLayoutEffect=function(e,r){return z.current.useLayoutEffect(e,r)},r.useMemo=function(e,r){return z.current.useMemo(e,r)},r.useReducer=function(e,r,t){return z.current.useReducer(e,r,t)},r.useRef=function(e){return z.current.useRef(e)},r.useState=function(e){return z.current.useState(e)},r.useSyncExternalStore=function(e,r,t){return z.current.useSyncExternalStore(e,r,t)},r.useTransition=function(){return z.current.useTransition()},r.version="18.2.0"},827378:(e,r,t)=>{e.exports=t(541535)},824246:(e,r,t)=>{e.exports=t(371426)},511151:(e,r,t)=>{t.d(r,{Z:()=>c,a:()=>u});var n=t(667294);const o={},i=n.createContext(o);function u(e){const r=n.useContext(i);return n.useMemo((function(){return"function"==typeof e?e(r):{...r,...e}}),[r,e])}function c(e){let r;return r=e.disableParentContext?"function"==typeof e.components?e.components(o):e.components||o:u(e.components),n.createElement(i.Provider,{value:r},e.children)}}}]);